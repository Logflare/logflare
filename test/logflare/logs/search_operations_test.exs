defmodule Logflare.Logs.SearchOperationsTest do
  use Logflare.DataCase, async: false

  import Ecto.Query
  import Logflare.Utils.Guards

  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Logs.SearchOperation, as: SO
  alias Logflare.Logs.SearchOperations
  alias Logflare.Lql.Parser
  alias Logflare.Lql.Rules.ChartRule
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Lql.Rules.SelectRule
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
          type: :events,
          partition_by: :timestamp
        }
      ]
    end

    test "unnest_recommended_fields/1 without metadata.level", %{so: so} do
      so =
        so
        |> SearchOperations.apply_query_defaults()
        |> SearchOperations.apply_select_rules()

      {:ok, {sql, _}} = BigQueryAdaptor.ecto_to_sql(so.query, [])

      refute sql =~ "UNNEST(t0.metadata)"
    end

    test "unnest_recommended_fields/1 with metadata.level", %{so: so} do
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

      Schema.update(pid, build(:log_event, metadata: %{"level" => "value"}), so.source)

      TestUtils.retry_assert(fn ->
        Cachex.clear(Logflare.SourceSchemas.Cache)

        so =
          so
          |> SearchOperations.apply_query_defaults()
          |> SearchOperations.apply_select_rules()

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

    test "text filter with count aggregation on event_message generates only one where clause",
         %{base_so: base_so} do
      text_filter = %FilterRule{
        path: "event_message",
        operator: :string_contains,
        value: "metrics",
        modifiers: %{}
      }

      chart_rule = %ChartRule{
        path: "event_message",
        aggregate: :count,
        period: :hour,
        value_type: :string
      }

      so = %{
        base_so
        | chart_rules: [chart_rule],
          lql_meta_and_msg_filters: [text_filter],
          query: from(base_so.source.bq_table_id)
      }

      so = SearchOperations.apply_numeric_aggs(so)
      {:ok, {sql, _}} = BigQueryAdaptor.ecto_to_sql(so.query, [])

      strpos_matches = Regex.scan(~r/STRPOS\([^,)]*event_message/i, sql)
      assert length(strpos_matches) == 1, "SQL: #{sql}"

      assert [%{params: [{"metrics", :any}]}] = so.query.wheres
    end
  end

  describe "apply_select_rules/1" do
    setup do
      insert(:plan)
      source = insert(:source, user: insert(:user), bq_table_id: "test_table")

      so = %SO{
        source: source,
        querystring: "",
        query: from("test_table"),
        chart_data_shape_id: nil,
        tailing?: false,
        partition_by: :timestamp,
        type: :events,
        lql_rules: [],
        lql_ts_filters: [],
        lql_meta_and_msg_filters: []
      }

      [so: so]
    end

    test "applies default SelectRules to query", %{so: so} do
      so =
        so
        |> SearchOperations.apply_query_defaults()
        |> SearchOperations.apply_select_rules()

      {:ok, {sql, _}} = BigQueryAdaptor.ecto_to_sql(so.query, [])

      assert sql =~ "t0.event_message"
      assert sql =~ "t0.timestamp"
      assert sql =~ "t0.id"
      refute sql =~ "UNNEST"
    end

    test "applies suggested keys as SelectRules to query", %{so: so} do
      GoogleApi.BigQuery.V2.Api.Tables
      |> stub(:bigquery_tables_patch, fn _conn, _project_id, _dataset_id, _table_name, _opts ->
        {:ok, %{}}
      end)

      source =
        so.source
        |> Ecto.Changeset.change(suggested_keys: "m.request_id, metadata.user_id")
        |> Logflare.Repo.update!()

      pid =
        start_supervised!(
          {Schema,
           source: source,
           bigquery_project_id: "some-project",
           bigquery_dataset_id: "some-dataset"}
        )

      # Create schema with the suggested key fields as top-level fields
      Schema.update(
        pid,
        build(:log_event, metadata: %{"request_id" => "req123", "user_id" => 123}),
        so.source
      )

      Logflare.Mailer
      |> expect(:deliver, 1, fn _ -> :ok end)

      TestUtils.retry_assert(fn ->
        Cachex.clear(Logflare.SourceSchemas.Cache)

        so =
          %{so | source: source}
          |> SearchOperations.apply_query_defaults()
          |> SearchOperations.apply_select_rules()

        {:ok, {sql, _}} = BigQueryAdaptor.ecto_to_sql(so.query, [])

        # Default fields should be present
        assert sql =~ "t0.event_message"
        assert sql =~ "t0.timestamp"
        assert sql =~ "t0.id"

        # Suggested keys should be selected
        assert sql =~ "request_id"
        assert sql =~ "user_id"
      end)
    end

    test "strips trailing exclamation point when working with `suggested_keys`", %{so: so} do
      GoogleApi.BigQuery.V2.Api.Tables
      |> stub(:bigquery_tables_patch, fn _conn, _project_id, _dataset_id, _table_name, _opts ->
        {:ok, %{}}
      end)

      source =
        so.source
        |> Ecto.Changeset.change(suggested_keys: "project!")
        |> Logflare.Repo.update!()

      pid =
        start_supervised!(
          {Schema,
           source: source,
           bigquery_project_id: "some-project",
           bigquery_dataset_id: "some-dataset"}
        )

      Schema.update(
        pid,
        build(:log_event, project: "my-project"),
        so.source
      )

      Logflare.Mailer
      |> expect(:deliver, 1, fn _ -> :ok end)

      TestUtils.retry_assert(fn ->
        Cachex.clear(Logflare.SourceSchemas.Cache)

        schema = TestUtils.build_bq_schema(%{"project" => "my-project"})
        {:ok, lql_rules} = Parser.parse("-project:NULL", schema)

        so =
          %{so | source: source, lql_rules: lql_rules}
          |> SearchOperations.apply_query_defaults()
          |> SearchOperations.apply_select_rules()

        {:ok, {sql, _}} = BigQueryAdaptor.ecto_to_sql(so.query, [])

        assert sql =~ "t0.event_message"
        assert sql =~ "t0.timestamp"
        assert sql =~ "t0.id"

        assert sql =~ "project"
        refute sql =~ "project!"
      end)
    end

    test "applies user rules and default SelectRules to query", %{so: so} do
      user_select_rule = %SelectRule{path: "user_id", wildcard: false}

      so =
        %{so | lql_rules: [user_select_rule]}
        |> SearchOperations.apply_query_defaults()
        |> SearchOperations.apply_select_rules()

      {:ok, {sql, _}} = BigQueryAdaptor.ecto_to_sql(so.query, [])

      assert sql =~ "t0.event_message"
      assert sql =~ "t0.timestamp"
      assert sql =~ "t0.id"
      assert sql =~ "user_id"
      refute sql =~ "UNNEST"
    end
  end

  describe "get_min_max_filter_timestamps/2" do
    setup do
      ts = ~N[2026-01-29 05:13:48.748909]

      ts_filters = [
        %FilterRule{
          path: "timestamp",
          operator: :=,
          value: ts,
          values: nil,
          modifiers: %{}
        }
      ]

      [ts: ts, ts_filters: ts_filters]
    end

    test "handles exact timestamp filter", %{ts: ts, ts_filters: filters} do
      %{min: min_ts, max: max_ts, message: nil} =
        Logflare.Logs.SearchOperations.Helpers.get_min_max_filter_timestamps(filters, :minute)

      assert min_ts == Timex.shift(ts, minutes: -1)
      assert max_ts == Timex.shift(ts, minutes: 1)
    end

    test "returns unbounded interval message for open timestamp", %{
      ts_filters: ts_filters
    } do
      filters = [%{hd(ts_filters) | operator: :>}]

      %{message: message} =
        Logflare.Logs.SearchOperations.Helpers.get_min_max_filter_timestamps(filters, :hour)

      assert message =~ "number of chart ticks is limited"
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

        {:ok,
         %{
           rows: [%{"test" => "data"}],
           total_rows: 1,
           query_string: "",
           bq_params: []
         }}
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
      assert captured_opts == [query_type: :search]
      assert result_so.rows == [%{"test" => "data"}]
      refute result_so.error
    end
  end
end
