defmodule Logflare.Backends.Adaptor.BigQueryAdaptorTest do
  use Logflare.DataCase
  use ExUnitProperties

  import Ecto.Query

  alias GoogleApi.BigQuery.V2.Api.Jobs, as: BqJobs
  alias GoogleApi.BigQuery.V2.Api.Tabledata, as: BqTabledata
  alias GoogleApi.BigQuery.V2.Api.Tables, as: BqTables
  alias GoogleApi.BigQuery.V2.Model
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.Backends.Backend
  alias Logflare.Backends.QueryError
  alias Logflare.Google

  @connection_test_message "Logflare BigQuery connection test. No action required."
  @connection_test_table :_logflare_connection_test
  @connection_test_table_name Atom.to_string(@connection_test_table)
  @connection_test_table_ttl :timer.hours(24)

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

  defp connection_test_table do
    %Model.Table{
      labels: %{
        "managed_by" => "logflare",
        "logflare_source" => @connection_test_table_name
      },
      requirePartitionFilter: true,
      schema: %Model.TableSchema{
        fields: [
          %Model.TableFieldSchema{name: "timestamp", type: "TIMESTAMP", mode: "REQUIRED"},
          %Model.TableFieldSchema{name: "id", type: "STRING", mode: "NULLABLE"},
          %Model.TableFieldSchema{name: "event_message", type: "STRING", mode: "NULLABLE"}
        ]
      },
      timePartitioning: %Model.TimePartitioning{
        expirationMs: Integer.to_string(@connection_test_table_ttl),
        field: "timestamp",
        type: "DAY"
      },
      type: "TABLE"
    }
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

      backend =
        insert(:backend,
          type: :bigquery,
          user: user,
          config: %{project_id: "test-project", dataset_id: "test_dataset"}
        )

      [backend: Backends.get_backend(backend.id)]
    end

    test "creates a fixed probe table and writes one labeled event through REST", %{
      backend: backend
    } do
      BqTables
      |> expect(:bigquery_tables_get, fn _conn,
                                         "test-project",
                                         "test_dataset",
                                         @connection_test_table_name ->
        {:error, %Tesla.Env{status: 404}}
      end)

      BqTables
      |> expect(:bigquery_tables_insert, fn _conn, project_id, dataset_id, opts ->
        assert project_id == "test-project"
        assert dataset_id == "test_dataset"

        assert %Model.Table{
                 tableReference: %Model.TableReference{
                   projectId: "test-project",
                   datasetId: "test_dataset",
                   tableId: @connection_test_table_name
                 },
                 labels: %{
                   "managed_by" => "logflare",
                   "logflare_source" => @connection_test_table_name
                 },
                 requirePartitionFilter: true,
                 schema: %Model.TableSchema{fields: fields},
                 timePartitioning: %Model.TimePartitioning{
                   field: "timestamp",
                   type: "DAY",
                   expirationMs: @connection_test_table_ttl
                 }
               } = table = opts[:body]

        assert Enum.map(fields, &{&1.name, &1.type, &1.mode}) == [
                 {"timestamp", "TIMESTAMP", "REQUIRED"},
                 {"id", "STRING", "NULLABLE"},
                 {"event_message", "STRING", "NULLABLE"}
               ]

        {:ok, table}
      end)

      BqTabledata
      |> expect(:bigquery_tabledata_insert_all, fn _conn,
                                                   project_id,
                                                   dataset_id,
                                                   table_name,
                                                   opts ->
        assert project_id == "test-project"
        assert dataset_id == "test_dataset"
        assert table_name == @connection_test_table_name

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

    test "reuses the probe table when it already exists", %{backend: backend} do
      BqTables
      |> expect(:bigquery_tables_get, fn _conn,
                                         "test-project",
                                         "test_dataset",
                                         @connection_test_table_name ->
        {:ok, connection_test_table()}
      end)

      Mimic.reject(Google.BigQuery, :create_table, 4)

      BqTabledata
      |> expect(:bigquery_tabledata_insert_all, fn _conn,
                                                   _project_id,
                                                   _dataset_id,
                                                   table_name,
                                                   _opts ->
        assert table_name == @connection_test_table_name
        {:ok, %Model.TableDataInsertAllResponse{insertErrors: []}}
      end)

      assert :ok = BigQueryAdaptor.test_connection(backend)
    end

    test "retries a not-found insert after creating the probe table", %{backend: backend} do
      BqTables
      |> expect(:bigquery_tables_get, fn _conn,
                                         "test-project",
                                         "test_dataset",
                                         @connection_test_table_name ->
        {:error, %Tesla.Env{status: 404}}
      end)

      Google.BigQuery
      |> expect(:create_table, fn @connection_test_table,
                                  "test_dataset",
                                  "test-project",
                                  @connection_test_table_ttl ->
        {:ok, connection_test_table()}
      end)

      test_pid = self()

      Google.BigQuery
      |> expect(:stream_batch!, 2, fn _context, [row] ->
        send(test_pid, {:insert_attempt, row.insertId})
        attempt = Process.get(:connection_test_insert_attempt, 0)
        Process.put(:connection_test_insert_attempt, attempt + 1)

        if attempt == 0 do
          {:error, %Tesla.Env{status: 404}}
        else
          {:ok, %Model.TableDataInsertAllResponse{insertErrors: nil}}
        end
      end)

      assert :ok = BigQueryAdaptor.test_connection(backend)
      assert_receive {:insert_attempt, insert_id}
      assert_receive {:insert_attempt, ^insert_id}
    end

    test "handles another request creating the probe table concurrently", %{backend: backend} do
      BqTables
      |> expect(:bigquery_tables_get, 2, fn _conn,
                                            "test-project",
                                            "test_dataset",
                                            @connection_test_table_name ->
        attempt = Process.get(:connection_test_table_get_attempt, 0)
        Process.put(:connection_test_table_get_attempt, attempt + 1)

        if attempt == 0 do
          {:error, %Tesla.Env{status: 404}}
        else
          {:ok, connection_test_table()}
        end
      end)

      Google.BigQuery
      |> expect(:create_table, fn @connection_test_table,
                                  "test_dataset",
                                  "test-project",
                                  @connection_test_table_ttl ->
        {:error, %Tesla.Env{status: 409}}
      end)

      Google.BigQuery
      |> expect(:stream_batch!, 2, fn _context, [row] ->
        attempt = Process.get(:concurrent_connection_test_insert_attempt, 0)
        Process.put(:concurrent_connection_test_insert_attempt, attempt + 1)

        if attempt == 0 do
          Process.put(:concurrent_connection_test_insert_id, row.insertId)
          {:error, %Tesla.Env{status: 404}}
        else
          assert Process.get(:concurrent_connection_test_insert_id) == row.insertId
          {:ok, %Model.TableDataInsertAllResponse{insertErrors: nil}}
        end
      end)

      assert :ok = BigQueryAdaptor.test_connection(backend)
    end

    test "rejects an unrelated table using the probe table name", %{backend: backend} do
      BqTables
      |> expect(:bigquery_tables_get, fn _conn,
                                         "test-project",
                                         "test_dataset",
                                         @connection_test_table_name ->
        {:ok, %Model.Table{type: "VIEW"}}
      end)

      Mimic.reject(Google.BigQuery, :create_table, 4)
      Mimic.reject(Google.BigQuery, :stream_batch!, 2)

      assert {:error, :probe_table_conflict} = BigQueryAdaptor.test_connection(backend)
    end

    test "returns an atom for insert failures", %{backend: backend} do
      BqTables
      |> stub(:bigquery_tables_get, fn _conn,
                                       "test-project",
                                       "test_dataset",
                                       @connection_test_table_name ->
        {:ok, connection_test_table()}
      end)

      responses = [
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

        assert {:error, ^expected_reason} = BigQueryAdaptor.test_connection(backend)
      end
    end

    test "returns an atom when the probe table cannot be read", %{backend: backend} do
      Mimic.reject(Google.BigQuery, :create_table, 4)
      Mimic.reject(Google.BigQuery, :stream_batch!, 2)

      responses = [
        {{:error, %Tesla.Env{status: 403, body: %{"error" => "forbidden"}}}, :http_client_error},
        {{:error, %Tesla.Env{status: 503, body: %{"error" => "unavailable"}}},
         :http_server_error},
        {{:error, :timeout}, :connection_error},
        {{:ok, :unexpected}, :unknown_error}
      ]

      for {response, expected_reason} <- responses do
        BqTables
        |> expect(:bigquery_tables_get, fn _conn,
                                           "test-project",
                                           "test_dataset",
                                           @connection_test_table_name ->
          response
        end)

        assert {:error, ^expected_reason} = BigQueryAdaptor.test_connection(backend)
      end
    end

    test "returns an atom when the probe table cannot be created", %{backend: backend} do
      BqTables
      |> stub(:bigquery_tables_get, fn _conn,
                                       "test-project",
                                       "test_dataset",
                                       @connection_test_table_name ->
        {:error, %Tesla.Env{status: 404}}
      end)

      Mimic.reject(Google.BigQuery, :stream_batch!, 2)

      responses = [
        {{:error, %Tesla.Env{status: 403, body: %{"error" => "forbidden"}}}, :http_client_error},
        {{:error, %Tesla.Env{status: 503, body: %{"error" => "unavailable"}}},
         :http_server_error},
        {{:error, :timeout}, :connection_error},
        {{:ok, :unexpected}, :unknown_error}
      ]

      for {response, expected_reason} <- responses do
        Google.BigQuery
        |> expect(:create_table, fn @connection_test_table,
                                    "test_dataset",
                                    "test-project",
                                    @connection_test_table_ttl ->
          response
        end)

        assert {:error, ^expected_reason} = BigQueryAdaptor.test_connection(backend)
      end
    end

    test "rejects incomplete configuration without making a request", %{backend: backend} do
      Mimic.reject(BqTables, :bigquery_tables_get, 5)
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
end
