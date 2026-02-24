defmodule Logflare.Backends.Adaptor.BigQueryAdaptorTest do
  use Logflare.DataCase

  import Ecto.Query

  alias Logflare.Backends.Backend
  alias Logflare.Backends.Adaptor.BigQueryAdaptor

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
              %{rows: [%{"event_message" => "test message", "value" => "123"}], total_rows: 1}} =
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
              %{rows: [%{"event_message" => "test message", "value" => "123"}], total_rows: 1}} =
               result
    end
  end
end
