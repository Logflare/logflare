defmodule Logflare.Backends.UserMonitoring.QueryTracesTest do
  use LogflareWeb.ConnCase, async: false

  import ExUnit.CaptureLog

  alias GoogleApi.BigQuery.V2.Api.Jobs
  alias GoogleApi.BigQuery.V2.Api.Tabledata
  alias GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.Backends.QueryError
  alias Logflare.Backends.SourceSup
  alias Logflare.Backends.UserMonitoring.QueryTraces
  alias Logflare.Endpoints
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
