defmodule Logflare.Backends.UserMonitoring.QueryTracesTest do
  use LogflareWeb.ConnCase, async: false
  use Oban.Testing, repo: Logflare.Repo

  import ExUnit.CaptureLog

  alias GoogleApi.BigQuery.V2.Api.Datasets
  alias GoogleApi.BigQuery.V2.Api.Jobs
  alias GoogleApi.BigQuery.V2.Api.Tabledata
  alias GoogleApi.BigQuery.V2.Api.Tables
  alias GoogleApi.BigQuery.V2.Model.Table
  alias GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse
  alias Logflare.Alerting
  alias Logflare.Alerting.AlertWorker
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.Backends.QueryError
  alias Logflare.Backends.SourceSup
  alias Logflare.Backends.UserMonitoring.QueryTraces
  alias Logflare.Endpoints
  alias Logflare.Google.BigQuery
  alias Logflare.Logs.LogEvents
  alias Logflare.Logs.Search
  alias Logflare.Logs.SearchOperation, as: SO
  alias Logflare.Lql.Rules.ChartRule
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Repo
  alias Logflare.Sources.Source.BigQuery.SchemaBuilder
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.Users

  setup do
    start_supervised!(AllLogsLogged)
    insert(:plan)
    pid = self()

    stub(Tabledata, :bigquery_tabledata_insert_all, fn _conn, _project, _dataset, _table, opts ->
      send(pid, {:insert_all, Enum.map(opts[:body].rows, & &1.json)})
      {:ok, %TableDataInsertAllResponse{insertErrors: nil}}
    end)

    :ok
  end

  describe "endpoint queries" do
    setup :start_traces_source

    test "trace a BigQuery query with bytes, params and labels", %{user: user} do
      stub(Jobs, :bigquery_jobs_query, fn _conn, _project, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"result" => "1"}])}
      end)

      endpoint =
        insert(:endpoint,
          user: user,
          query: "SELECT @project AS result",
          parsed_labels: %{"team" => "core"}
        )

      assert {:ok, %{rows: [%{"result" => "1"}]}} =
               Endpoints.run_query(endpoint, %{"project" => "abc", "limit" => 10})

      assert_receive {:insert_all, [trace]}, 5_000
      endpoint_id = endpoint.id

      assert %{
               "event_message" => "query.endpoint",
               "query_type" => "endpoint",
               "endpoint_language" => "bq_sql",
               "backend_type" => "bigquery",
               "status" => "ok",
               "total_bytes_processed" => 1024,
               "endpoint_id" => ^endpoint_id,
               "duration_ms" => duration_ms,
               "query" => query,
               "params" => [%{"name" => "project", "value" => "abc"}],
               "labels" => [%{"name" => "team", "value" => "core"}]
             } = trace

      assert is_integer(duration_ms)
      assert query =~ "SELECT"
      refute Map.has_key?(trace, "error")
    end

    test "trace a ClickHouse query with its backend", %{user: user} do
      backend = insert(:backend, user: user, type: :clickhouse)

      stub(ClickHouseAdaptor, :execute_query, fn _backend, _query, _opts ->
        {:ok, QueryResult.new([%{"result" => "1"}], %{total_bytes_processed: 100})}
      end)

      endpoint =
        insert(:endpoint, user: user, language: :ch_sql, query: "SELECT 1", backend: backend)

      assert {:ok, _result} = Endpoints.run_query(endpoint)

      assert_receive {:insert_all, [trace]}, 5_000
      backend_id = backend.id

      assert %{
               "endpoint_language" => "ch_sql",
               "backend_type" => "clickhouse",
               "backend_id" => ^backend_id,
               "total_bytes_processed" => 100
             } = trace
    end

    test "trace a failed query and keep the error result", %{user: user} do
      backend = insert(:backend, user: user, type: :clickhouse)
      error = %QueryError{kind: :invalid_query, raw_error: :bad, backend: ClickHouseAdaptor}
      error = %{error | description: "Unknown table"}

      stub(ClickHouseAdaptor, :execute_query, fn _backend, _query, _opts -> {:error, error} end)

      endpoint =
        insert(:endpoint, user: user, language: :ch_sql, query: "SELECT 1", backend: backend)

      assert {:error, ^error} = Endpoints.run_query(endpoint)

      assert_receive {:insert_all, [trace]}, 5_000
      assert %{"status" => "error", "error" => "Unknown table"} = trace
      refute Map.has_key?(trace, "total_bytes_processed")
    end
  end

  describe "endpoint requests" do
    setup :start_traces_source

    test "trace declared params without credentials or undeclared params", %{
      conn: conn,
      user: user
    } do
      stub(Jobs, :bigquery_jobs_query, fn _conn, _project, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"result" => "1"}])}
      end)

      endpoint =
        insert(:endpoint,
          user: user,
          enable_auth: true,
          query: "SELECT @project AS result, @api_key AS key"
        )

      params = %{project: "abc", api_key: user.api_key, foo: "1"}

      conn
      |> get(~p"/endpoints/query/#{endpoint.token}?#{params}")
      |> json_response(200)

      assert_receive {:insert_all, [trace]}, 5_000
      assert trace["params"] == [%{"name" => "project", "value" => "abc"}]
    end
  end

  describe "search queries" do
    setup :start_traces_source

    setup %{user: user} do
      stub(Jobs, :bigquery_jobs_query, fn _conn, _project, _opts ->
        {:ok, TestUtils.gen_bq_response([])}
      end)

      schema = TestUtils.build_bq_schema(%{"metadata" => %{"level" => "error"}})
      source = insert(:source, user: user, bq_table_id: "test_table")
      insert(:source_schema, source: source, bigquery_schema: schema)
      %{source: source}
    end

    test "trace the events and chart queries of a search", %{source: source} do
      so = search_op(source, tailing?: false)

      assert {:ok, _result} = Search.search(so)
      assert_receive {:insert_all, [events_trace]}, 5_000

      assert {:ok, _result} = Search.aggs(so)
      assert_receive {:insert_all, [chart_trace]}, 5_000

      source_id = source.id
      source_name = source.name
      source_token = Atom.to_string(source.token)

      assert %{
               "event_message" => "query.search",
               "query_type" => "search",
               "search_kind" => "events",
               "lql" => "m.level:error",
               "backend_type" => "bigquery",
               "source_id" => ^source_id,
               "source_name" => ^source_name,
               "source_token" => ^source_token,
               "status" => "ok",
               "total_bytes_processed" => 0,
               "query" => events_sql
             } = events_trace

      assert %{"search_kind" => "aggregates", "query" => chart_sql} = chart_trace
      assert events_sql =~ "SELECT"
      assert chart_sql =~ "SELECT"
      refute Map.has_key?(events_trace, "endpoint_language")
    end

    test "trace only the first query of a tail", %{source: source} do
      assert {:ok, _result} =
               source |> search_op(tailing?: true, tailing_initial?: true) |> Search.search()

      assert_receive {:insert_all, [_trace]}, 5_000

      repeated_tick = search_op(source, tailing?: true, tailing_initial?: false)
      assert {:ok, _result} = Search.search(repeated_tick)
      assert {:ok, _result} = Search.aggs(repeated_tick)

      marker = search_op(source, tailing?: false, querystring: "m.level:marker")
      assert {:ok, _result} = Search.search(marker)

      assert_receive {:insert_all, rows}, 5_000
      assert [%{"lql" => "m.level:marker"}] = rows
    end
  end

  describe "alert queries" do
    setup :start_traces_source

    setup %{user: user} do
      stub(Jobs, :bigquery_jobs_query, fn _conn, _project, _opts ->
        {:ok, TestUtils.gen_bq_response([])}
      end)

      %{alert: :alert |> insert(user: user) |> Repo.preload(:user)}
    end

    test "trace scheduled and manual alert queries", %{alert: alert} do
      assert {:ok, _result} = Alerting.execute_alert_query(alert)
      assert_receive {:insert_all, [scheduled]}, 5_000

      assert {:ok, _result} = Alerting.execute_alert_query(alert, trigger: :manual)
      assert_receive {:insert_all, [manual]}, 5_000

      alert_id = alert.id
      alert_name = alert.name

      assert %{
               "event_message" => "query.alert",
               "alert_id" => ^alert_id,
               "alert_name" => ^alert_name,
               "alert_language" => "bq_sql",
               "alert_trigger" => "scheduled",
               "backend_type" => "bigquery",
               "status" => "ok",
               "total_bytes_processed" => 0,
               "query" => query
             } = scheduled

      assert query =~ "current_date"
      assert %{"alert_trigger" => "manual"} = manual
    end

    test "trace an alert triggered now as manual", %{alert: alert} do
      alert_id = alert.id

      assert {:ok, %Oban.Job{args: %{trigger: "manual"} = args}} =
               Alerting.trigger_alert_now(alert)

      assert :ok = perform_job(AlertWorker, args)

      assert_receive {:insert_all, [%{"alert_id" => ^alert_id, "alert_trigger" => "manual"}]},
                     5_000
    end
  end

  describe "single event fetches" do
    setup :start_traces_source

    test "trace a single event fetch", %{user: user} do
      event_id = Ecto.UUID.generate()

      stub(Jobs, :bigquery_jobs_query, fn _conn, _project, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"id" => event_id, "event_message" => "hello"}])}
      end)

      schema = TestUtils.build_bq_schema(%{"metadata" => %{"level" => "error"}})
      source = insert(:source, user: user)
      insert(:source_schema, source: source, bigquery_schema: schema)
      now = DateTime.to_string(DateTime.utc_now())

      assert %{"event_message" => "hello"} =
               LogEvents.fetch_event_by_id(source.token, event_id,
                 partitions_range: [now, now],
                 lql: "m.level:error"
               )

      assert_receive {:insert_all, [trace]}, 5_000
      source_id = source.id

      assert %{
               "event_message" => "query.event",
               "event_id" => ^event_id,
               "lql" => "m.level:error",
               "source_id" => ^source_id,
               "backend_type" => "bigquery",
               "status" => "ok",
               "query" => query
             } = trace

      assert query =~ ".id ="
    end
  end

  describe "ClickHouse provisioning" do
    setup :start_traces_source

    test "trace a failed table provisioning with its DDL", %{user: user} do
      config = %{
        url: "http://localhost:1",
        port: 1,
        username: "user",
        password: "password",
        database: "default"
      }

      backend = insert(:backend, user: user, type: :clickhouse, config: config)

      capture_log(fn ->
        assert {:error, _error} = ClickHouseAdaptor.provision_ingest_tables(backend)
      end)

      assert_receive {:insert_all, [trace]}, 5_000
      backend_id = backend.id

      assert %{
               "event_message" => "query.provision",
               "backend_type" => "clickhouse",
               "backend_id" => ^backend_id,
               "status" => "error",
               "query" => query
             } = trace

      assert query =~ "CREATE TABLE IF NOT EXISTS"
    end
  end

  describe "Postgres provisioning" do
    setup :start_traces_source

    setup %{user: user} do
      repo = Application.get_env(:logflare, Repo)

      url =
        "postgresql://#{repo[:username]}:#{repo[:password]}@#{repo[:hostname]}/#{repo[:database]}"

      config = %{url: url, schema: nil}

      source = insert(:source, user: user)
      backend = insert(:backend, type: :postgres, user: user, sources: [source], config: config)
      on_exit(fn -> PostgresAdaptor.destroy_instance({source, backend}) end)

      %{source: source, backend: backend}
    end

    test "trace each provisioning statement of a source once", %{
      user: user,
      source: source,
      backend: backend
    } do
      assert :ok = PostgresAdaptor.create_events_table({source, backend})

      traces = receive_traces(2)
      backend_id = backend.id
      source_id = source.id

      for trace <- traces do
        assert %{
                 "event_message" => "query.provision",
                 "backend_type" => "postgres",
                 "backend_id" => ^backend_id,
                 "source_id" => ^source_id,
                 "status" => "ok"
               } = trace
      end

      assert traces |> Enum.map(& &1["query"]) |> Enum.any?(&(&1 =~ "CREATE TABLE IF NOT EXISTS"))
      assert traces |> Enum.map(& &1["query"]) |> Enum.any?(&(&1 =~ "CREATE INDEX IF NOT EXISTS"))

      assert :ok = PostgresAdaptor.create_events_table({source, backend})
      assert_next_trace_is_marker(user)
    end

    test "skip provisioning of system sources", %{user: user, backend: backend} do
      system_source = insert(:source, user: user, system_source: true, system_source_type: :logs)
      on_exit(fn -> PostgresAdaptor.destroy_instance({system_source, backend}) end)

      assert :ok = PostgresAdaptor.create_events_table({system_source, backend})
      assert_next_trace_is_marker(user)
    end
  end

  describe "BigQuery provisioning" do
    setup :start_traces_source

    setup %{user: user} do
      %{source: insert(:source, user: user)}
    end

    test "trace dataset and table creation once, with already existing as success", %{
      user: user,
      source: source
    } do
      dataset_id = "dataset_#{System.unique_integer([:positive])}"

      stub(Datasets, :bigquery_datasets_insert, fn _conn, _project, _opts ->
        {:error, %Tesla.Env{status: 409, body: ~s({"error":{"message":"Already Exists"}})}}
      end)

      stub(Tables, :bigquery_tables_insert, fn _conn, _project, _dataset, _opts ->
        {:ok, %Table{}}
      end)

      assert {:ok, %Table{}} =
               BigQuery.init_table!(user.id, source.token, "project", 1_000, "US", dataset_id)

      traces = receive_traces(2)
      source_id = source.id

      assert %{"status" => "ok", "backend_type" => "bigquery"} =
               Enum.find(traces, &(&1["operation"] == "datasets.insert"))

      assert %{"status" => "ok", "source_id" => ^source_id, "event_message" => "query.provision"} =
               Enum.find(traces, &(&1["operation"] == "tables.insert"))

      assert {:ok, %Table{}} =
               BigQuery.init_table!(user.id, source.token, "project", 1_000, "US", dataset_id)

      assert_next_trace_is_marker(user)
    end

    test "trace a failed table patch with the API error message", %{source: source} do
      stub(Tables, :bigquery_tables_patch, fn _conn, _project, _dataset, _table, _opts ->
        {:error, %Tesla.Env{status: 400, body: ~s({"error":{"message":"Invalid clustering"}})}}
      end)

      assert {:error, %Tesla.Env{status: 400}} =
               BigQuery.patch_table_clustering(source.token, ["a"], "dataset", "project")

      assert_receive {:insert_all, [trace]}, 5_000

      assert %{
               "operation" => "tables.patch",
               "status" => "error",
               "error" => "Invalid clustering"
             } = trace

      refute Map.has_key?(trace, "query")
    end

    test "trace a table deletion", %{source: source} do
      stub(Tables, :bigquery_tables_delete, fn _conn, _project, _dataset, _table ->
        {:ok, nil}
      end)

      assert {:ok, nil} = BigQuery.delete_table(source.token)

      assert_receive {:insert_all, [%{"operation" => "tables.delete", "status" => "ok"}]}, 5_000
    end

    test "skip operations on system sources", %{user: user} do
      system_source = insert(:source, user: user, system_source: true, system_source_type: :logs)

      stub(Tables, :bigquery_tables_patch, fn _conn, _project, _dataset, _table, _opts ->
        {:ok, %Table{}}
      end)

      assert {:ok, %Table{}} =
               BigQuery.patch_table_clustering(system_source.token, ["a"], "dataset", "project")

      assert_next_trace_is_marker(user)
    end
  end

  describe "measure_once/4" do
    setup :start_traces_source

    test "trace a successful step only the first time", %{user: user} do
      key = make_ref()
      result = {:ok, QueryResult.new([], %{})}

      assert measure_once(user, key, "CREATE TABLE a", result) == result
      assert_receive {:insert_all, [%{"query" => "CREATE TABLE a"}]}, 5_000

      assert measure_once(user, key, "CREATE TABLE a", result) == result
      assert_next_trace_is_marker(user)
    end

    test "trace failed runs until a run succeeds", %{user: user} do
      key = make_ref()
      ok = {:ok, QueryResult.new([], %{})}

      assert measure_once(user, key, "CREATE TABLE b", {:error, "boom"}) == {:error, "boom"}
      assert_receive {:insert_all, [%{"status" => "error"}]}, 5_000

      assert measure_once(user, key, "CREATE TABLE b", ok) == ok
      assert_receive {:insert_all, [%{"status" => "ok"}]}, 5_000

      assert measure_once(user, key, "CREATE TABLE b", ok) == ok
      assert_next_trace_is_marker(user)
    end

    test "run without tracing when there is no user" do
      result = {:ok, QueryResult.new([], %{})}

      assert QueryTraces.measure_once(
               nil,
               make_ref(),
               %{query_type: :provision, query: "x"},
               fn ->
                 result
               end
             ) == result
    end
  end

  describe "measure/3" do
    setup :start_traces_source

    test "omit bytes when the backend does not report them", %{user: user} do
      result = {:ok, QueryResult.new([], %{})}

      assert QueryTraces.measure(user.id, %{query_type: :endpoint, query: "SELECT 1"}, fn ->
               result
             end) == result

      assert_receive {:insert_all, [trace]}, 5_000
      assert %{"status" => "ok", "query" => "SELECT 1"} = trace
      refute Map.has_key?(trace, "total_bytes_processed")
    end

    test "record nothing when system monitoring is off", %{user: user} do
      {:ok, user} = Users.update_user_allowed(user, %{"system_monitoring" => false})
      result = {:ok, QueryResult.new([], %{})}

      assert QueryTraces.measure(user.id, %{query_type: :endpoint, query: "SELECT 1"}, fn ->
               result
             end) == result

      refute_receive {:insert_all, _rows}, 500
    end

    test "drop the trace and keep the result when the worker is saturated", %{user: user} do
      attach_trace_drops()
      previous = Application.get_env(:logflare, QueryTraces)
      Application.put_env(:logflare, QueryTraces, max_queue_len: 0)
      on_exit(fn -> restore_env(previous) end)
      result = {:error, "boom"}

      assert QueryTraces.measure(user.id, %{query_type: :endpoint, query: "SELECT 1"}, fn ->
               result
             end) == result

      assert_receive {:dropped, %{count: 1}, %{reason: :saturated}}
      refute_receive {:insert_all, _rows}, 500
    end

    test "drop the trace and keep the result when the worker is down", %{user: user} do
      attach_trace_drops()
      pid = Process.whereis(QueryTraces)
      Process.unregister(QueryTraces)
      on_exit(fn -> Process.register(pid, QueryTraces) end)
      result = {:ok, QueryResult.new([%{"a" => 1}], %{})}

      assert QueryTraces.measure(user.id, %{query_type: :endpoint, query: "SELECT 1"}, fn ->
               result
             end) == result

      assert_receive {:dropped, %{count: 1}, %{reason: :unavailable}}
      refute_receive {:insert_all, _rows}, 500
    end

    test "drop the trace and keep the worker running when the ingest fails", %{user: user} do
      set_mimic_global()
      attach_trace_drops()
      pid = Process.whereis(QueryTraces)
      stub(Users.Cache, :get, fn _user_id -> raise "boom" end)

      capture_log(fn ->
        QueryTraces.measure(user.id, %{query_type: :endpoint, query: "SELECT 1"}, fn ->
          {:ok, QueryResult.new([], %{})}
        end)

        assert_receive {:dropped, %{count: 1}, %{reason: :failed}}, 5_000
      end)

      assert Process.whereis(QueryTraces) == pid
      assert Process.alive?(pid)
    end
  end

  describe "build_event/1" do
    test "keep the same schema for different param and label names" do
      schema_a =
        event_schema(%{params: %{"project" => "abc"}, labels: %{"team" => "core"}})

      schema_b =
        event_schema(%{params: %{"region" => "eu", "limit" => 10}, labels: %{"env" => "prod"}})

      assert schema_a == schema_b
    end
  end

  @spec search_op(Logflare.Sources.Source.t(), keyword()) :: SO.t()
  defp search_op(source, opts) do
    lql_rules = [
      %ChartRule{path: "timestamp", aggregate: :count, period: :minute, value_type: :datetime},
      %FilterRule{path: "metadata.level", operator: :=, value: "error", modifiers: %{}}
    ]

    %{
      source: source,
      lql_rules: lql_rules,
      querystring: "m.level:error",
      chart_data_shape_id: nil
    }
    |> Map.merge(Map.new(opts))
    |> SO.new()
  end

  @spec measure_once(Logflare.User.t(), reference(), String.t(), QueryTraces.query_result()) ::
          QueryTraces.query_result()
  defp measure_once(user, key, query, result) do
    QueryTraces.measure_once(user.id, key, %{query_type: :provision, query: query}, fn ->
      result
    end)
  end

  @spec receive_traces(pos_integer(), [map()]) :: [map()]
  defp receive_traces(count, traces \\ [])
  defp receive_traces(count, traces) when length(traces) >= count, do: traces

  defp receive_traces(count, traces) do
    assert_receive {:insert_all, rows}, 5_000
    receive_traces(count, traces ++ rows)
  end

  @spec assert_next_trace_is_marker(Logflare.User.t()) :: term()
  defp assert_next_trace_is_marker(user) do
    QueryTraces.measure(user.id, %{query_type: :provision, query: "marker"}, fn ->
      {:ok, QueryResult.new([], %{})}
    end)

    assert_receive {:insert_all, rows}, 5_000
    assert [%{"query" => "marker"}] = rows
  end

  @spec event_schema(map()) :: map()
  defp event_schema(attrs) do
    %{query_type: :endpoint, query: "SELECT 1"}
    |> Map.merge(attrs)
    |> QueryTraces.build_event()
    |> SchemaBuilder.build_table_schema(SchemaBuilder.initial_table_schema())
  end

  @spec attach_trace_drops() :: :ok
  defp attach_trace_drops do
    test_pid = self()
    ref = make_ref()
    event = [:logflare, :user_monitoring, :query_traces, :dropped]

    :telemetry.attach(
      ref,
      event,
      fn ^event, measurements, metadata, _config ->
        send(test_pid, {:dropped, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(ref) end)
  end

  @spec restore_env(keyword() | nil) :: :ok
  defp restore_env(nil), do: Application.delete_env(:logflare, QueryTraces)
  defp restore_env(previous), do: Application.put_env(:logflare, QueryTraces, previous)

  @spec start_traces_source(map()) :: map()
  defp start_traces_source(_context) do
    user = insert(:user, system_monitoring: true)
    source = insert(:source, user: user, system_source: true, system_source_type: :traces)
    start_supervised!({SourceSup, source})
    %{user: user}
  end
end
