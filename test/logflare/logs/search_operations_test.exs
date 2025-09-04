defmodule Logflare.Logs.SearchOperationsTest do
  use Logflare.DataCase, async: false
  import Ecto.Query

  alias Logflare.Logs.SearchOperations
  alias Logflare.Sources.Source.BigQuery.Schema
  alias Logflare.Logs.SearchOperation, as: SO
  alias Logflare.Lql.Rules.ChartRule
  alias Logflare.Lql.Rules.FilterRule

  describe "unnesting metadata if present" do
    setup do
      insert(:plan)
      source = insert(:source, user: insert(:user), bq_table_id: "1")

      [
        so: %Logflare.Logs.SearchOperation{
          source: source,
          querystring: "SELECT * FROM test",
          chart_data_shape_id: nil,
          tailing?: false,
          partition_by: :timestamp
        }
      ]
    end

    test "unnest_log_level/1", %{so: so} do
      so =
        so
        |> SearchOperations.apply_query_defaults()
        |> SearchOperations.unnest_log_level()

      {sql, _} = Logflare.EctoQueryBQ.SQL.to_sql_params(so.query)

      refute sql =~ "UNNEST(t0.metadata)"
    end

    test "unnest_log_level/1 with metadata", %{so: so} do
      GoogleApi.BigQuery.V2.Api.Tables
      |> stub(:bigquery_tables_patch, fn _conn, _project_id, _dataset_id, _table_name, _opts ->
        {:ok, %{}}
      end)

      Logflare.Mailer
      |> expect(:deliver, 1, fn _ -> :ok end)

      pid =
        start_supervised!(
          {Schema,
           source: so.source,
           bigquery_project_id: "some-project",
           bigquery_dataset_id: "some-dataset"}
        )

      Schema.update(pid, build(:log_event, metadata: %{"level" => "value"}))

      TestUtils.retry_assert(fn ->
        Cachex.clear(Logflare.SourceSchemas.Cache)

        so =
          so
          |> SearchOperations.apply_query_defaults()
          |> SearchOperations.unnest_log_level()

        {sql, _} = Logflare.EctoQueryBQ.SQL.to_sql_params(so.query)

        assert sql =~ "UNNEST(t0.metadata)"
      end)
    end
  end

  describe "chart aggregation query generation" do
    setup do
      insert(:plan)
      source = insert(:source, user: insert(:user), bq_table_id: "test_table")

      base_so = %SO{
        source: source,
        querystring: "",
        chart_data_shape_id: nil,
        tailing?: false,
        partition_by: :timestamp,
        type: :aggregates,
        lql_ts_filters: [],
        lql_meta_and_msg_filters: []
      }

      [base_so: base_so]
    end

    test "top-level field aggregation should reference base table, not joined table", %{
      base_so: base_so
    } do
      chart_rule = %ChartRule{
        path: "value",
        aggregate: :max,
        period: :minute,
        value_type: :integer
      }

      nested_filter = %FilterRule{
        path: "attributes.name",
        operator: :"~",
        value: "jose",
        modifiers: %{}
      }

      so = %{
        base_so
        | chart_rules: [chart_rule],
          lql_meta_and_msg_filters: [nested_filter],
          query: from(base_so.source.bq_table_id)
      }

      so = SearchOperations.apply_numeric_aggs(so)
      {sql, _} = Logflare.EctoQueryBQ.SQL.to_sql_params(so.query)

      assert sql =~ "MAX(t0.value)"
      refute sql =~ "MAX(f1.value)"
    end

    test "nested field aggregation should reference joined table correctly", %{base_so: base_so} do
      chart_rule = %ChartRule{
        path: "metadata.level",
        aggregate: :count,
        period: :minute,
        value_type: :string
      }

      so = %{base_so | chart_rules: [chart_rule], query: from(base_so.source.bq_table_id)}
      so = SearchOperations.apply_numeric_aggs(so)
      {sql, _} = Logflare.EctoQueryBQ.SQL.to_sql_params(so.query)

      assert sql =~ "UNNEST"
      assert sql =~ "COUNT(f1.level)"
    end
  end
end
