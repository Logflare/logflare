defmodule Logflare.Backends.Adaptor.BigQueryAdaptorTest do
  use Logflare.DataCase
  use ExUnitProperties

  import Ecto.Query
  import ExUnit.CaptureLog

  alias GoogleApi.BigQuery.V2.Api.Jobs, as: BqJobs
  alias Logflare.Backends.Backend
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.Backends.QueryError

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

      assert {:error,
              %QueryError{
                code: :invalid_query,
                backend: Logflare.Backends.Adaptor.BigQueryAdaptor,
                message: "Unrecognized name: notthere at [1:8]",
                description: nil,
                raw_error: %{
                  "message" => "Unrecognized name: notthere at [1:8]",
                  "reason" => "invalidQuery"
                }
              }} =
               BigQueryAdaptor.execute_query(
                 {user.bigquery_project_id || "test-project", user.bigquery_dataset_id, user.id},
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
                code: :invalid_query,
                backend: Logflare.Backends.Adaptor.BigQueryAdaptor,
                message:
                  "Query exceeded limit for bytes billed: 2000000000. 20004857600 or higher required.",
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
                code: :connection_error,
                backend: Logflare.Backends.Adaptor.BigQueryAdaptor,
                message: "timeout",
                description: nil,
                raw_error: :timeout
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

    test "keeps the default timeout for searches without a reservation and for non-search queries" do
      user = insert(:user, bigquery_dataset_id: "test_dataset")

      BigQueryAdaptor.execute_query(
        {"test-project", user.bigquery_dataset_id, user.id},
        {"select 1", []},
        query_type: :search
      )

      assert_received {:timeouts, 25_000, 25_000}

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
  end

  describe "reservation error logging" do
    setup do
      insert(:plan, name: "Free", type: "standard")
      user = insert(:user, bigquery_dataset_id: "test_dataset")
      [user: user]
    end

    test "logs a warning for a reservation-not-found error", %{user: user} do
      body =
        ~s|{"error":{"message":"User specified reservation projects/p/locations/l/reservations/missing is not found","status":"NOT_FOUND"}}|

      stub(BqJobs, :bigquery_jobs_query, fn _conn, _proj, _opts ->
        {:error, %Tesla.Env{status: 404, body: body}}
      end)

      log =
        capture_log([level: :warning], fn ->
          BigQueryAdaptor.execute_query(
            {"test-project", user.bigquery_dataset_id, user.id},
            {"select 1", []},
            reservation: "projects/p/locations/l/reservations/missing"
          )
        end)

      assert log =~ "Possible BigQuery reservation error"
    end

    test "logs a warning for a permission-denied reservation error", %{user: user} do
      body =
        ~s|{"error":{"message":"Access Denied: Reservation projects/p/locations/l/reservations/r: Permission bigquery.reservations.use denied on reservation projects/p/locations/l/reservations/r (or it may not exist)","status":"PERMISSION_DENIED"}}|

      stub(BqJobs, :bigquery_jobs_query, fn _conn, _proj, _opts ->
        {:error, %Tesla.Env{status: 403, body: body}}
      end)

      log =
        capture_log([level: :warning], fn ->
          BigQueryAdaptor.execute_query(
            {"test-project", user.bigquery_dataset_id, user.id},
            {"select 1", []},
            []
          )
        end)

      assert log =~ "Possible BigQuery reservation error"
    end

    test "logs a warning for a slot/region reservation error", %{user: user} do
      body =
        ~s|{"error":{"message":"Cannot run query: project does not have the reservation in the data region or no slots are configured"}}|

      stub(BqJobs, :bigquery_jobs_query, fn _conn, _proj, _opts ->
        {:error, %Tesla.Env{status: 400, body: body}}
      end)

      log =
        capture_log([level: :warning], fn ->
          BigQueryAdaptor.execute_query(
            {"test-project", user.bigquery_dataset_id, user.id},
            {"select 1", []},
            []
          )
        end)

      assert log =~ "Possible BigQuery reservation error"
    end

    test "does not log centrally for alerts queries, which log their own errors", %{user: user} do
      body =
        ~s|{"error":{"message":"User specified reservation projects/p/locations/l/reservations/missing is not found","status":"NOT_FOUND"}}|

      stub(BqJobs, :bigquery_jobs_query, fn _conn, _proj, _opts ->
        {:error, %Tesla.Env{status: 404, body: body}}
      end)

      log =
        capture_log([level: :warning], fn ->
          BigQueryAdaptor.execute_query(
            {"test-project", user.bigquery_dataset_id, user.id},
            {"select 1", []},
            query_type: :alerts
          )
        end)

      refute log =~ "Possible BigQuery reservation error"
    end

    test "does not log a warning for unrelated BigQuery errors", %{user: user} do
      body =
        ~s|{"error":{"message":"Table test-project:test_dataset.foo not found","status":"NOT_FOUND"}}|

      stub(BqJobs, :bigquery_jobs_query, fn _conn, _proj, _opts ->
        {:error, %Tesla.Env{status: 404, body: body}}
      end)

      log =
        capture_log([level: :warning], fn ->
          BigQueryAdaptor.execute_query(
            {"test-project", user.bigquery_dataset_id, user.id},
            {"select 1", []},
            []
          )
        end)

      refute log =~ "Possible BigQuery reservation error"
    end
  end
end
