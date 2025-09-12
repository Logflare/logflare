defmodule Logflare.Logs.SearchOperationsTest do
  use Logflare.DataCase, async: false

  import Ecto.Query
  import Logflare.Utils.Guards

  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Logs.SearchOperation, as: SO
  alias Logflare.Logs.SearchOperations
  alias Logflare.Lql.Rules.ChartRule
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Sources.Source.BigQuery.Schema

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

      {:ok, {sql, _}} = BigQueryAdaptor.ecto_to_sql(so.query, [])

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

        {:ok, {sql, _}} = BigQueryAdaptor.ecto_to_sql(so.query, [])

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
      {:ok, {sql, _}} = BigQueryAdaptor.ecto_to_sql(so.query, [])

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
      {:ok, {sql, _}} = BigQueryAdaptor.ecto_to_sql(so.query, [])

      assert sql =~ "UNNEST"
      assert sql =~ "COUNT(f1.level)"
    end
  end

  describe "backend adaptor integration" do
    setup do
      insert(:plan)
      source = insert(:source, user: insert(:user), bq_table_id: "test_table")

      base_so = %SO{
        source: source,
        querystring: "",
        query: from("test_table"),
        chart_data_shape_id: nil,
        tailing?: false,
        partition_by: :timestamp,
        type: :events,
        lql_ts_filters: [],
        lql_meta_and_msg_filters: []
      }

      [base_so: base_so]
    end

    test "do_query/1 uses BigQuery backend adaptor", %{base_so: base_so} do
      Mimic.stub(BigQueryAdaptor, :execute_query, fn identifier, query, opts ->
        Process.put(:captured_identifier, identifier)
        Process.put(:captured_query, query)
        Process.put(:captured_opts, opts)

        {:ok, %{rows: [%{"test" => "data"}], total_rows: 1}}
      end)

      result_so = SearchOperations.do_query(base_so)

      captured_identifier = Process.get(:captured_identifier)
      captured_query = Process.get(:captured_query)
      captured_opts = Process.get(:captured_opts)

      assert {project_id, dataset_id, user_id} = captured_identifier
      assert is_non_empty_binary(project_id)
      assert is_non_empty_binary(dataset_id)
      assert is_integer(user_id)
      assert user_id == base_so.source.user.id
      assert %Ecto.Query{} = captured_query
      assert captured_opts == []
      assert result_so.rows == [%{"test" => "data"}]
      refute result_so.error
    end
  end
end
