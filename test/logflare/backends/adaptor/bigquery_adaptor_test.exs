defmodule Logflare.Backends.Adaptor.BigQueryAdaptorTest do
  use Logflare.DataCase
  use ExUnitProperties

  import Ecto.Query

  alias GoogleApi.BigQuery.V2.Api.Jobs, as: BqJobs
  alias GoogleApi.BigQuery.V2.Api.Tabledata, as: BqTabledata
  alias GoogleApi.BigQuery.V2.Api.Tables, as: BqTables
  alias GoogleApi.BigQuery.V2.Model
  alias Logflare.Backends.Backend
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Backends.Adaptor.BigQueryAdaptor.GoogleApiClient
  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.Backends.QueryError
  alias Logflare.Google

  @connection_test_message "Logflare BigQuery connection test. No action required."

  # Characters illegal in a BigQuery dataset identifier: SQL delimiters,
  # identifier-quoting characters, whitespace, and shell metacharacters.
  @injection_chars ~c";`.'\" -/\\#!@$%^&*()+={}[]|<>?,~"

  defp dataset_id_with_injection do
    gen all prefix <- string(:alphanumeric, min_length: 1),
            bad_char <- member_of(@injection_chars),
            suffix <- string(:alphanumeric) do
      prefix <> <<bad_char>> <> suffix
    end
  end

  describe "validate_config/1" do
    test "accepts valid dataset_id and project_id" do
      changeset =
        BigQueryAdaptor.cast_config(%{dataset_id: "my_dataset_1", project_id: "my-project-id"})

      assert BigQueryAdaptor.validate_config(changeset).valid?
    end

    property "rejects dataset_id containing any injection character" do
      check all bad <- dataset_id_with_injection() do
        changeset = BigQueryAdaptor.cast_config(%{dataset_id: bad, project_id: "my-project-id"})
        validated = BigQueryAdaptor.validate_config(changeset)
        refute validated.valid?
        assert Keyword.has_key?(validated.errors, :dataset_id)
      end
    end

    test "rejects project_id with injection characters" do
      for bad <- [
            "evil;drop",
            "evil`proj",
            "UPPERCASE_proj",
            "ab",
            "a" <> String.duplicate("b", 30)
          ] do
        changeset = BigQueryAdaptor.cast_config(%{dataset_id: "valid_dataset", project_id: bad})
        validated = BigQueryAdaptor.validate_config(changeset)
        refute validated.valid?
        assert Keyword.has_key?(validated.errors, :project_id)
      end
    end

    test "allows nil dataset_id and project_id" do
      changeset = BigQueryAdaptor.cast_config(%{})
      assert BigQueryAdaptor.validate_config(changeset).valid?
    end
  end

  describe "test_connection/1" do
    setup do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user: user, bq_storage_write_api: true)

      backend =
        insert(:backend,
          type: :bigquery,
          user: user,
          sources: [source],
          config: %{project_id: "test-project", dataset_id: "test_dataset"}
        )

      [backend: Backends.get_backend(backend.id), source: source, user: user]
    end

    test "writes one labeled event to the attached source table through REST", %{
      backend: backend,
      source: source
    } do
      Mimic.reject(BqTables, :bigquery_tables_get, 4)
      Mimic.reject(Google.BigQuery, :create_table, 4)
      Mimic.reject(GoogleApiClient, :append_rows, 3)

      BqTabledata
      |> expect(:bigquery_tabledata_insert_all, fn _conn,
                                                   project_id,
                                                   dataset_id,
                                                   table_name,
                                                   opts ->
        assert project_id == "test-project"
        assert dataset_id == "test_dataset"
        assert table_name == BigQueryAdaptor.format_table_name(source.token)

        assert %Model.TableDataInsertAllRequest{
                 ignoreUnknownValues: true,
                 skipInvalidRows: true,
                 rows: [%Model.TableDataInsertAllRequestRows{} = row]
               } = opts[:body]

        assert row.insertId == row.json["id"]
        assert is_binary(row.insertId)
        assert %DateTime{} = row.json["timestamp"]
        assert row.json["event_message"] == @connection_test_message

        {:ok, %Model.TableDataInsertAllResponse{insertErrors: nil}}
      end)

      assert :ok = BigQueryAdaptor.test_connection(backend)
    end

    test "uses a rule source when no source is attached directly", %{user: user} do
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :bigquery,
          user: user,
          config: %{project_id: "rule-project", dataset_id: "rule_dataset"}
        )

      insert(:rule, backend: backend, source: source)

      Google.BigQuery
      |> expect(:stream_batch!, fn context, [_row] ->
        assert context == %{
                 bigquery_project_id: "rule-project",
                 bigquery_dataset_id: "rule_dataset",
                 source_token: source.token
               }

        {:ok, %Model.TableDataInsertAllResponse{insertErrors: nil}}
      end)

      assert :ok = BigQueryAdaptor.test_connection(Backends.get_backend(backend.id))
    end

    test "prefers a direct source over an older rule source", %{user: user} do
      rule_source = insert(:source, user: user)
      direct_source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :bigquery,
          user: user,
          sources: [direct_source],
          config: %{project_id: "test-project", dataset_id: "test_dataset"}
        )

      insert(:rule, backend: backend, source: rule_source)

      Google.BigQuery
      |> expect(:stream_batch!, fn %{source_token: source_token}, [_row] ->
        assert source_token == direct_source.token
        {:ok, %Model.TableDataInsertAllResponse{insertErrors: nil}}
      end)

      assert rule_source.id < direct_source.id
      assert :ok = BigQueryAdaptor.test_connection(Backends.get_backend(backend.id))
    end

    test "selects the oldest directly attached source deterministically", %{user: user} do
      first_source = insert(:source, user: user)
      second_source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :bigquery,
          user: user,
          sources: [second_source, first_source],
          config: %{project_id: "test-project", dataset_id: "test_dataset"}
        )

      Google.BigQuery
      |> expect(:stream_batch!, fn %{source_token: source_token}, [_row] ->
        assert source_token == first_source.token
        {:ok, %Model.TableDataInsertAllResponse{insertErrors: nil}}
      end)

      assert first_source.id < second_source.id
      assert :ok = BigQueryAdaptor.test_connection(Backends.get_backend(backend.id))
    end

    test "requires an attached direct or rule source", %{user: user} do
      backend =
        insert(:backend,
          type: :bigquery,
          user: user,
          config: %{project_id: "test-project", dataset_id: "test_dataset"}
        )

      Mimic.reject(BqTables, :bigquery_tables_get, 4)
      Mimic.reject(Google.BigQuery, :create_table, 4)
      Mimic.reject(Google.BigQuery, :stream_batch!, 2)

      assert {:error, :source_required} =
               BigQueryAdaptor.test_connection(Backends.get_backend(backend.id))
    end

    test "ignores source associations owned by another user", %{user: user} do
      other_user = insert(:user)
      direct_source = insert(:source, user: other_user)
      rule_source = insert(:source, user: other_user)

      backend =
        insert(:backend,
          type: :bigquery,
          user: user,
          sources: [direct_source],
          config: %{project_id: "test-project", dataset_id: "test_dataset"}
        )

      insert(:rule, backend: backend, source: rule_source)

      Mimic.reject(Google.BigQuery, :stream_batch!, 2)

      assert {:error, :source_required} =
               BigQueryAdaptor.test_connection(Backends.get_backend(backend.id))
    end

    test "normalizes REST insert responses to atom reasons", %{backend: backend} do
      responses = [
        {{:ok, %Model.TableDataInsertAllResponse{insertErrors: []}}, :ok},
        {{:ok, %Model.TableDataInsertAllResponse{insertErrors: [%{index: 0}]}}, :insert_error},
        {{:error, %Tesla.Env{status: 404, body: %{"error" => "not found"}}}, :http_client_error},
        {{:error, %Tesla.Env{status: 403, body: %{"error" => "forbidden"}}}, :http_client_error},
        {{:error, %Tesla.Env{status: 503, body: %{"error" => "unavailable"}}},
         :http_server_error},
        {{:error, :timeout}, :connection_error},
        {{:error, :unexpected}, :unknown_error},
        {{:ok, :unexpected}, :unknown_error}
      ]

      for {response, expected_reason} <- responses do
        Google.BigQuery
        |> expect(:stream_batch!, fn _context, [_row] -> response end)

        expected = if expected_reason == :ok, do: :ok, else: {:error, expected_reason}
        assert BigQueryAdaptor.test_connection(backend) == expected
      end
    end

    test "rejects incomplete configuration without making a request", %{backend: backend} do
      Mimic.reject(BqTables, :bigquery_tables_get, 4)
      Mimic.reject(Google.BigQuery, :create_table, 4)
      Mimic.reject(Google.BigQuery, :stream_batch!, 2)

      for config <- [
            %{},
            %{project_id: "", dataset_id: "test_dataset"},
            %{project_id: "test-project", dataset_id: nil}
          ] do
        assert {:error, :invalid_config} =
                 BigQueryAdaptor.test_connection(%{backend | config: config})
      end
    end
  end

  describe "ecto_to_sql/2" do
    test "converts Ecto query to BigQuery SQL format" do
      query =
        from("test_table")
        |> select([t], %{id: t.id, value: t.value})
        |> where([t], t.id > ^1)

      {:ok, {sql, params}} = BigQueryAdaptor.ecto_to_sql(query, [])

      assert is_binary(sql)
      assert is_list(params)

      # Should convert PostgreSQL quoted identifiers to BigQuery format
      refute sql =~ ~r/"[\w\.]+"/

      # Should convert PostgreSQL parameters ($1) to BigQuery question marks (?)
      assert sql =~ "?"
      refute sql =~ "$1"

      # Should contain basic query structure
      assert sql =~ "SELECT"
      assert sql =~ "FROM test_table"
      assert sql =~ "WHERE"
      assert sql =~ "t0.id >"

      # Parameters should be in BigQuery format
      assert length(params) == 1
      [param | _] = params
      assert %GoogleApi.BigQuery.V2.Model.QueryParameter{} = param
      assert is_binary(param.parameterType.type)
      assert param.parameterValue.value == 1
    end

    test "converts complex query with joins and aggregates" do
      query =
        from(t in "test_table", as: :base)
        |> join(:left, [base: t], f in fragment("UNNEST(?)", t.metadata), as: :metadata, on: true)
        |> select([base: t, metadata: f], %{
          timestamp: t.timestamp,
          count: count(f.level)
        })
        |> where([base: t], t.timestamp > ^DateTime.utc_now())
        |> group_by([base: t], t.timestamp)

      {:ok, {sql, params}} = BigQueryAdaptor.ecto_to_sql(query, [])

      assert is_binary(sql)
      assert is_list(params)

      # Should handle complex SQL structures
      assert sql =~ "UNNEST"
      assert sql =~ "count"
      assert sql =~ "GROUP BY"
      assert sql =~ "LEFT"

      # Should convert datetime parameter
      assert length(params) == 1
      [param | _] = params
      assert %GoogleApi.BigQuery.V2.Model.QueryParameter{} = param
      assert param.parameterType.type == "STRING"
      assert is_binary(param.parameterValue.value)
    end

    test "handles query with no parameters" do
      query =
        from("test_table")
        |> select([t], %{id: t.id, value: t.value})
        |> where([t], t.id > 0)

      {:ok, {sql, params}} = BigQueryAdaptor.ecto_to_sql(query, [])

      assert is_binary(sql)
      assert params == []
      assert sql =~ "WHERE (t0.id > 0)"
    end

    test "handles query conversion errors gracefully" do
      # Create an invalid query that should fail conversion
      invalid_query = %Ecto.Query{from: nil}

      assert {:error, _reason} = BigQueryAdaptor.ecto_to_sql(invalid_query, [])
    end

    test "converts subqueries correctly" do
      query = from("some-table") |> select([:id])
      {:ok, {sql, _params}} = BigQueryAdaptor.ecto_to_sql(query, [])
      assert sql == "SELECT s0.id FROM some-table AS s0", "table is unquoted"

      subquery1 = from(t in "some-table", select: %{id: t.id, name: t.name})
      subquery2 = from(s in subquery(subquery1), select: %{count: fragment("COUNT(*)")})

      query =
        from(main in "some-table",
          join: sub in subquery(subquery2),
          on: true,
          select: [main.id, sub.count]
        )

      {:ok, {sql, _params}} = BigQueryAdaptor.ecto_to_sql(query, [])

      expected_sql =
        "SELECT s0.id, s1.count FROM some-table AS s0 INNER JOIN (SELECT COUNT(*) AS count FROM (SELECT sss0.id AS id, sss0.name AS name FROM some-table AS sss0) AS ss0) AS s1 ON TRUE"

      assert sql == expected_sql
    end
  end

  describe "execute_query/3 with Ecto queries" do
    setup do
      insert(:plan, name: "Free", type: "standard")
      user = insert(:user, bigquery_dataset_id: "test_dataset")

      stub(BqJobs, :bigquery_jobs_query, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"event_message" => "test message", "value" => "123"}])}
      end)

      [user: user]
    end

    test "execute_query handles Ecto queries with project/dataset/user_id identifier", %{
      user: user
    } do
      query =
        from("test_table")
        |> select([t], %{id: t.id, value: t.value})
        |> where([t], t.id > ^1)

      result =
        BigQueryAdaptor.execute_query(
          {user.bigquery_project_id || "test-project", user.bigquery_dataset_id, user.id},
          query,
          []
        )

      assert {:ok,
              %QueryResult{
                rows: [%{"event_message" => "test message", "value" => "123"}],
                total_rows: 1
              }} =
               result
    end

    test "execute_query handles Ecto queries with Backend struct identifier", %{user: user} do
      backend = %Backend{
        user_id: user.id,
        config: %{project_id: "test-project", dataset_id: "test-dataset"}
      }

      query = from("test_table") |> select([t], t.value)
      result = BigQueryAdaptor.execute_query(backend, query, [])

      assert {:ok,
              %QueryResult{
                rows: [%{"event_message" => "test message", "value" => "123"}],
                total_rows: 1
              }} =
               result
    end

    test "execute_query translates errors to QueryError", %{user: user} do
      stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, _opts ->
        {:error,
         TestUtils.gen_bq_error("Unrecognized name: notthere at [1:8]",
           reason: "invalidQuery"
         )}
      end)

      project_id = user.bigquery_project_id || "test-project"

      assert {:error,
              %QueryError{
                kind: :invalid_query,
                backend: Logflare.Backends.Adaptor.BigQueryAdaptor,
                description: nil,
                raw_error: %{
                  "message" => "Unrecognized name: notthere at [1:8]",
                  "reason" => "invalidQuery"
                }
              }} =
               BigQueryAdaptor.execute_query(
                 {project_id, user.bigquery_dataset_id, user.id},
                 {"select notthere", []},
                 []
               )
    end

    test "execute_query normalizes bytes billed limit errors", %{user: user} do
      stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, _opts ->
        {:error,
         TestUtils.gen_bq_error(
           "Query exceeded limit for bytes billed: 2000000000. 20004857600 or higher required.",
           reason: "billingTierLimitExceeded"
         )}
      end)

      assert {:error,
              %QueryError{
                kind: :backend_error,
                backend: Logflare.Backends.Adaptor.BigQueryAdaptor,
                description: nil,
                raw_error: %{
                  "message" =>
                    "Query exceeded limit for bytes billed: 2000000000. 20004857600 or higher required.",
                  "reason" => "billingTierLimitExceeded"
                }
              }} =
               BigQueryAdaptor.execute_query(
                 {user.bigquery_project_id || "test-project", user.bigquery_dataset_id, user.id},
                 {"select count(*) from logs", []},
                 []
               )
    end

    test "execute_query normalizes transport timeout errors", %{user: user} do
      stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, _opts ->
        {:error, :timeout}
      end)

      assert {:error,
              %QueryError{
                kind: :connection_error,
                backend: Logflare.Backends.Adaptor.BigQueryAdaptor,
                description: nil,
                raw_error: :timeout
              }} =
               BigQueryAdaptor.execute_query(
                 {user.bigquery_project_id || "test-project", user.bigquery_dataset_id, user.id},
                 {"select count(*) from logs", []},
                 []
               )
    end

    test "execute_query normalizes job timeout errors", %{user: user} do
      message = "Job execution was cancelled: Job timed out"

      stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, _opts ->
        {:error,
         TestUtils.gen_bq_error(message,
           code: 499,
           status: "CANCELLED",
           errors: [%{"domain" => "global", "message" => message, "reason" => "stopped"}]
         )}
      end)

      assert {:error,
              %QueryError{
                kind: :timeout,
                backend: Logflare.Backends.Adaptor.BigQueryAdaptor,
                description: nil,
                raw_error: %{"message" => ^message, "status" => "CANCELLED"}
              }} =
               BigQueryAdaptor.execute_query(
                 {user.bigquery_project_id || "test-project", user.bigquery_dataset_id, user.id},
                 {"select count(*) from logs", []},
                 query_type: :search
               )
    end

    test "execute_query only treats job timeouts as timeouts for searches", %{user: user} do
      message = "Job execution was cancelled: Job timed out"

      stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, _opts ->
        {:error,
         TestUtils.gen_bq_error(message,
           code: 499,
           status: "CANCELLED",
           errors: [%{"domain" => "global", "message" => message, "reason" => "stopped"}]
         )}
      end)

      for query_opts <- [[query_type: :endpoint], [query_type: :alert], []] do
        assert {:error, %QueryError{kind: :backend_error}} =
                 BigQueryAdaptor.execute_query(
                   {user.bigquery_project_id || "test-project", user.bigquery_dataset_id,
                    user.id},
                   {"select count(*) from logs", []},
                   query_opts
                 )
      end
    end

    test "execute_query normalizes timeout reason errors", %{user: user} do
      stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, _opts ->
        {:error, TestUtils.gen_bq_error("Operation timeout exceeded", reason: "timeout")}
      end)

      assert {:error, %QueryError{kind: :timeout}} =
               BigQueryAdaptor.execute_query(
                 {user.bigquery_project_id || "test-project", user.bigquery_dataset_id, user.id},
                 {"select count(*) from logs", []},
                 query_type: :search
               )
    end

    test "execute_query does not treat cancelled jobs as timeouts", %{user: user} do
      message = "Job execution was cancelled: User requested cancellation"

      stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, _opts ->
        {:error,
         TestUtils.gen_bq_error(message,
           code: 499,
           status: "CANCELLED",
           errors: [%{"domain" => "global", "message" => message, "reason" => "stopped"}]
         )}
      end)

      assert {:error, %QueryError{kind: :backend_error}} =
               BigQueryAdaptor.execute_query(
                 {user.bigquery_project_id || "test-project", user.bigquery_dataset_id, user.id},
                 {"select count(*) from logs", []},
                 query_type: :search
               )
    end

    test "execute_query maps non-invalid BigQuery reasons as backend errors", %{user: user} do
      stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, _opts ->
        {:error, TestUtils.gen_bq_error("backend unavailable", reason: "backendError")}
      end)

      assert {:error,
              %QueryError{
                kind: :backend_error,
                backend: Logflare.Backends.Adaptor.BigQueryAdaptor,
                description: nil,
                raw_error: %{
                  "message" => "backend unavailable",
                  "reason" => "backendError"
                }
              }} =
               BigQueryAdaptor.execute_query(
                 {user.bigquery_project_id || "test-project", user.bigquery_dataset_id, user.id},
                 {"select count(*) from logs", []},
                 []
               )
    end
  end

  describe "build_base_query_opts reservation" do
    setup do
      insert(:plan, name: "Free", type: "standard")
      pid = self()

      user =
        insert(:user,
          bigquery_dataset_id: "test_dataset",
          bigquery_reservation_search: "projects/p/locations/l/reservations/search"
        )

      stub(BqJobs, :bigquery_jobs_query, fn _conn, _proj_id, opts ->
        send(pid, {:reservation, opts[:body].reservation})
        {:ok, TestUtils.gen_bq_response()}
      end)

      [user: user]
    end

    test "explicit :reservation in opts overrides query_type-based reservation", %{user: user} do
      override = "projects/p/locations/l/reservations/override"

      BigQueryAdaptor.execute_query(
        {user.bigquery_project_id || "test-project", user.bigquery_dataset_id, user.id},
        {"select 1", []},
        query_type: :search,
        reservation: override
      )

      assert_received {:reservation, ^override}
    end

    test "falls back to query_type-based reservation when :reservation is nil", %{user: user} do
      BigQueryAdaptor.execute_query(
        {user.bigquery_project_id || "test-project", user.bigquery_dataset_id, user.id},
        {"select 1", []},
        query_type: :search
      )

      assert_received {:reservation, "projects/p/locations/l/reservations/search"}
    end

    test "reservation is nil when query_type is not :search or :alerts and no override", %{
      user: user
    } do
      BigQueryAdaptor.execute_query(
        {user.bigquery_project_id || "test-project", user.bigquery_dataset_id, user.id},
        {"select 1", []},
        []
      )

      assert_received {:reservation, nil}
    end
  end

  describe "search query timeouts" do
    setup do
      insert(:plan, name: "Free", type: "standard")
      pid = self()

      stub(BqJobs, :bigquery_jobs_query, fn _conn, _proj_id, opts ->
        send(pid, {:timeouts, opts[:body].jobTimeoutMs, opts[:body].timeoutMs})
        {:ok, TestUtils.gen_bq_response()}
      end)

      :ok
    end

    test "uses a 60s timeout for :search queries with a custom reservation" do
      user =
        insert(:user,
          bigquery_dataset_id: "test_dataset",
          bigquery_reservation_search: "projects/p/locations/l/reservations/search"
        )

      BigQueryAdaptor.execute_query(
        {"test-project", user.bigquery_dataset_id, user.id},
        {"select 1", []},
        query_type: :search
      )

      assert_received {:timeouts, 60_000, 60_000}
    end

    test "uses a 60s timeout for :search queries" do
      user = insert(:user, bigquery_dataset_id: "test_dataset")

      BigQueryAdaptor.execute_query(
        {"test-project", user.bigquery_dataset_id, user.id},
        {"select 1", []},
        query_type: :search
      )

      assert_received {:timeouts, 60_000, 60_000}
    end

    test "keeps the default timeout for non-seareh queries" do
      user_with_reservation =
        insert(:user,
          bigquery_dataset_id: "test_dataset",
          bigquery_reservation_alerts: "projects/p/locations/l/reservations/alerts"
        )

      BigQueryAdaptor.execute_query(
        {"test-project", user_with_reservation.bigquery_dataset_id, user_with_reservation.id},
        {"select 1", []},
        query_type: :alerts
      )

      assert_received {:timeouts, 25_000, 25_000}
    end

    test "uses a 60s timeout for :search queries with an explicit reservation override" do
      user = insert(:user, bigquery_dataset_id: "test_dataset")

      BigQueryAdaptor.execute_query(
        {"test-project", user.bigquery_dataset_id, user.id},
        {"select 1", []},
        query_type: :search,
        reservation: "projects/p/locations/l/reservations/override"
      )

      assert_received {:timeouts, 60_000, 60_000}
    end

    test "uses a 60s timeout for :endpoint queries with a custom reservation" do
      user = insert(:user, bigquery_dataset_id: "test_dataset")

      BigQueryAdaptor.execute_query(
        {"test-project", user.bigquery_dataset_id, user.id},
        {"select 1", []},
        query_type: :endpoint,
        reservation: "projects/p/locations/l/reservations/endpoint"
      )

      assert_received {:timeouts, 60_000, 60_000}
    end

    test "keeps the default timeout for :endpoint queries without a reservation" do
      user = insert(:user, bigquery_dataset_id: "test_dataset")

      BigQueryAdaptor.execute_query(
        {"test-project", user.bigquery_dataset_id, user.id},
        {"select 1", []},
        query_type: :endpoint
      )

      assert_received {:timeouts, 25_000, 25_000}
    end
  end

  describe "sanitize_config_for_display/1" do
    test "preserves project_id and dataset_id" do
      config = %{project_id: "my-project", dataset_id: "my_dataset"}

      assert config == BigQueryAdaptor.sanitize_config_for_display(config)
    end
  end
end
