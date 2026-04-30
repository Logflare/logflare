defmodule Logflare.Backends.Adaptor.BigQueryAdaptorTest do
  use Logflare.DataCase
  use ExUnitProperties

  import Ecto.Query

  alias Logflare.Backends.Backend
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Backends.Adaptor.QueryResult

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
      changeset = BigQueryAdaptor.cast_config(%{dataset_id: "my_dataset_1", project_id: "my-project-id"})
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
      for bad <- ["evil;drop", "evil`proj", "UPPERCASE_proj", "ab", "a" <> String.duplicate("b", 30)] do
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

      stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, _opts ->
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

      stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, opts ->
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
end
