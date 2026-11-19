defmodule Logflare.Backends.UserMonitoring.QueryTracesTest do
  use Logflare.DataCase, async: false

  alias GoogleApi.BigQuery.V2.Api.Jobs
  alias GoogleApi.BigQuery.V2.Api.Tabledata
  alias GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.Backends.QueryError
  alias Logflare.Backends.SourceSup
  alias Logflare.Backends.UserMonitoring.QueryTraces
  alias Logflare.Endpoints
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
               "query_language" => "bq_sql",
               "backend_type" => "bigquery",
               "status" => "ok",
               "total_bytes_processed" => 1024,
               "endpoint_id" => ^endpoint_id,
               "duration_ms" => duration_ms,
               "query" => query,
               "params" => [%{"project" => "abc", "limit" => "10"}],
               "labels" => [%{"team" => "core"}]
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
               "query_language" => "ch_sql",
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
  end

  @spec start_traces_source(map()) :: map()
  defp start_traces_source(_context) do
    user = insert(:user, system_monitoring: true)
    source = insert(:source, user: user, system_source: true, system_source_type: :traces)
    start_supervised!({SourceSup, source})
    %{user: user}
  end
end
