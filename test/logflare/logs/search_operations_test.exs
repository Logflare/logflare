defmodule Logflare.Logs.SearchOperationsTest do
  use Logflare.DataCase, async: false

  import Ecto.Query
  import Logflare.Utils.Guards
  import Logflare.TestUtils

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.Logs.SearchOperation, as: SO
  alias Logflare.Logs.SearchOperations
  alias Logflare.Lql.Parser
  alias Logflare.Lql.Rules.ChartRule
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Lql.Rules.SelectRule

  @postgres_search_attrs %{
    source: nil,
    querystring: "",
    query: nil,
    chart_data_shape_id: nil,
    tailing?: false,
    tailing_initial?: nil,
    partition_by: :timestamp,
    type: :events,
    backend_type: :postgres,
    lql_rules: [],
    lql_ts_filters: [],
    lql_meta_and_msg_filters: []
  }

  setup do
    insert(:plan)

    [user: insert(:user)]
  end

  describe "unnesting metadata if present" do
    setup %{user: user} do
      source = insert(:source, user: user, bq_table_id: "1")

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
      insert(:source_schema,
        source: so.source,
        bigquery_schema: TestUtils.build_bq_schema(%{"metadata" => %{"level" => "value"}})
      )

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
    setup %{user: user} do
      source = insert(:source, user: user, bq_table_id: "test_table")

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

  describe "postgres chart aggregation" do
    setup_single_tenant(backend_type: :postgres)

    setup %{user: user} do
      source = insert(:source, user: user)

      base_so =
        @postgres_search_attrs
        |> Map.merge(%{source: source, type: :aggregates})
        |> SO.new()

      [base_so: base_so]
    end

    test "chart-path filters are excluded from WHERE clause", %{base_so: base_so} do
      chart_filter = %FilterRule{
        path: "event_message",
        operator: :string_contains,
        value: "metrics",
        modifiers: %{}
      }

      non_chart_filter = %FilterRule{
        path: "metadata.level",
        operator: :=,
        value: "error",
        modifiers: %{}
      }

      chart_rule = %ChartRule{
        path: "event_message",
        aggregate: :count,
        period: :minute,
        value_type: :string
      }

      so = %{
        base_so
        | chart_rules: [chart_rule],
          lql_meta_and_msg_filters: [chart_filter, non_chart_filter],
          query: from("test_table")
      }

      so = SearchOperations.apply_numeric_aggs(so)

      assert length(so.query.wheres) == 1
    end

    test "generates expected SQL for all aggregate types", %{base_so: base_so} do
      for agg <- [:count, :avg, :sum, :max, :countd, :p50, :p95, :p99] do
        chart_rule = %ChartRule{
          path: "event_message",
          aggregate: agg,
          period: :minute,
          value_type: :string
        }

        so = %{
          base_so
          | chart_rules: [chart_rule],
            query: from("test_table")
        }

        so = SearchOperations.apply_numeric_aggs(so)
        {:ok, {sql, _params}} = PostgresAdaptor.ecto_to_sql(so.query, [])
        sql = String.downcase(sql)

        expected_aggregate_sql =
          case agg do
            :count -> ~s|count(t0."timestamp")|
            :countd -> ~s|count(distinct t0."event_message")|
            :p50 -> ~s|percentile_cont(|
            :p95 -> ~s|percentile_cont(|
            :p99 -> ~s|percentile_cont(|
            _ -> ~s|#{agg}(t0."event_message")|
          end

        assert sql =~ expected_aggregate_sql
      end
    end
  end

  describe "postgres query defaults and rules" do
    setup_single_tenant(backend_type: :postgres)

    setup %{user: user} do
      source = insert(:source, user: user)

      so =
        %{@postgres_search_attrs | source: source}
        |> SO.new()

      [so: so]
    end

    test "apply_query_defaults/1 uses the postgres table name", %{so: so} do
      so = SearchOperations.apply_query_defaults(so)

      {:ok, {sql, _params}} = PostgresAdaptor.ecto_to_sql(so.query, [])

      assert sql =~ ~s|FROM "#{PostgresAdaptor.table_name(so.source)}"|
      assert sql =~ ~s|ORDER BY l0."timestamp" DESC|
      assert sql =~ ~s|LIMIT 100|
    end

    test "apply_select_rules/1 uses postgres dialect defaults", %{so: so} do
      so =
        so
        |> SearchOperations.apply_query_defaults()
        |> SearchOperations.apply_select_rules()

      {:ok, {sql, _params}} = PostgresAdaptor.ecto_to_sql(so.query, [])

      assert sql =~ ~s|SELECT l0."timestamp", l0."id", l0."event_message"|
    end

    test "apply_filters/1 uses postgres dialect for top-level fields", %{so: so} do
      filter = %FilterRule{
        path: "event_message",
        operator: :=,
        value: "error",
        modifiers: %{}
      }

      so =
        %{so | lql_meta_and_msg_filters: [filter]}
        |> SearchOperations.apply_query_defaults()
        |> SearchOperations.apply_filters()

      {:ok, {sql, params}} = PostgresAdaptor.ecto_to_sql(so.query, [])

      assert sql =~ ~s|l0."event_message"|
      assert params == ["error"]
    end
  end

  describe "postgres timestamp filter rules" do
    setup_single_tenant(backend_type: :postgres)

    setup %{user: user} do
      source = insert(:source, user: user)

      base_so =
        @postgres_search_attrs
        |> Map.merge(%{
          source: source,
          tailing_initial?: false,
          query: from("test_table")
        })
        |> SO.new()

      [base_so: base_so]
    end

    test "events live tail query applies the 10 minute timestamp window", %{base_so: base_so} do
      before_call = DateTime.utc_now()

      so =
        %{base_so | tailing?: true}
        |> SearchOperations.apply_timestamp_filter_rules()

      after_call = DateTime.utc_now()
      [%{params: [{cutoff, _type}]}] = so.query.wheres

      assert DateTime.compare(cutoff, DateTime.add(before_call, -601, :second)) == :gt
      assert DateTime.compare(cutoff, DateTime.add(after_call, -599, :second)) == :lt
    end

    test "events initial tail query applies the default 2 day window", %{base_so: base_so} do
      before_call = DateTime.utc_now()

      so =
        %{base_so | tailing?: true, tailing_initial?: true}
        |> SearchOperations.apply_timestamp_filter_rules()

      after_call = DateTime.utc_now()
      [%{params: [{cutoff, _type}]}] = so.query.wheres

      assert DateTime.compare(cutoff, DateTime.add(before_call, -(2 * 24 * 3600 + 1), :second)) ==
               :gt

      assert DateTime.compare(cutoff, DateTime.add(after_call, -(2 * 24 * 3600 - 1), :second)) ==
               :lt
    end

    test "events explicit timestamp filters use postgres filter rules", %{base_so: base_so} do
      min = ~U[2026-01-29 04:13:48.748909Z]
      max = ~U[2026-01-29 06:13:48.748909Z]

      timestamp_filter = %FilterRule{
        path: "timestamp",
        operator: :range,
        values: [min, max],
        modifiers: %{}
      }

      so =
        %{base_so | lql_ts_filters: [timestamp_filter]}
        |> SearchOperations.apply_timestamp_filter_rules()

      [%{params: params}] = so.query.wheres

      assert [{^min, _}, {^max, _}] = params
    end

    test "aggregate query without filter applys min/max", %{base_so: base_so} do
      chart_rule = %ChartRule{
        path: "timestamp",
        aggregate: :count,
        period: :minute,
        value_type: :datetime
      }

      so =
        %{base_so | type: :aggregates, chart_rules: [chart_rule], query: nil}
        |> SearchOperations.apply_timestamp_filter_rules()

      assert so.query.from.source == {PostgresAdaptor.table_name(base_so.source), nil}
      assert length(so.query.wheres) == 1

      [%{params: params}] = so.query.wheres

      assert [{%DateTime{}, _}, {%DateTime{}, _}] = params
    end

    test "aggregate query uses filters and timestamp", %{base_so: base_so} do
      min = ~U[2026-01-29 04:13:48.748909Z]
      max = ~U[2026-01-29 06:13:48.748909Z]

      chart_rule = %ChartRule{
        path: "timestamp",
        aggregate: :count,
        period: :minute,
        value_type: :datetime
      }

      timestamp_filter = %FilterRule{
        path: "timestamp",
        operator: :range,
        values: [min, max],
        modifiers: %{}
      }

      so =
        %{
          base_so
          | type: :aggregates,
            chart_rules: [chart_rule],
            lql_ts_filters: [timestamp_filter],
            query: nil
        }
        |> SearchOperations.apply_timestamp_filter_rules()

      assert length(so.query.wheres) == 2

      [first_where, second_where] = so.query.wheres

      assert [{^min, _}, {^max, _}] = first_where.params
      assert [{^min, _}, {^max, _}] = second_where.params
    end
  end

  describe "postgres backend adaptor integration" do
    setup_single_tenant(backend_type: :postgres)

    setup %{user: user} do
      Mimic.copy(PostgresAdaptor)

      source = insert(:source, user: user)
      backend = build(:backend, type: :postgres)

      base_so =
        %{@postgres_search_attrs | source: source, query: from("test_table")}
        |> SO.new()

      [backend: backend, base_so: base_so]
    end

    test "do_query/1 uses Postgres backend adaptor and passes through event rows", %{
      backend: backend,
      base_so: base_so
    } do
      timestamp_us = DateTime.to_unix(~U[2026-01-29 05:13:48.748909Z], :microsecond)
      inserted_at_us = DateTime.to_unix(~U[2026-01-29 05:14:48.748909Z], :microsecond)
      seen_at_us = DateTime.to_unix(~U[2026-01-29 05:15:48.748909Z], :microsecond)

      rows = [
        %{
          "event_message" => "postgres event",
          "timestamp" => timestamp_us,
          "inserted_at" => inserted_at_us,
          "log_date" => "2026-01-29",
          "metadata" => %{"level" => "error", "seen_at" => seen_at_us},
          "tags" => ["2026-01-29", inserted_at_us]
        }
      ]

      Backends
      |> expect(:get_default_backend, fn user ->
        assert user.id == base_so.source.user.id
        backend
      end)

      PostgresAdaptor
      |> expect(:execute_query, fn ^backend, %Ecto.Query{} = query, opts ->
        assert opts == [query_type: :search]
        assert %Ecto.Query{} = query

        {:ok, QueryResult.new(rows, %{total_rows: length(rows)})}
      end)

      result_so = SearchOperations.do_query(base_so)

      assert result_so.rows == rows
      refute result_so.error
    end

    test "do_query/1 propagates postgres backend errors", %{backend: backend, base_so: base_so} do
      Backends
      |> expect(:get_default_backend, fn _user -> backend end)

      PostgresAdaptor
      |> expect(:execute_query, fn ^backend, %Ecto.Query{}, [query_type: :search] ->
        raise Postgrex.Error,
          postgres: %{
            code: "26000",
            pg_code: :invalid_sql_statement_name,
            message: "connection refused",
            severity: "ERROR"
          }
      end)

      assert_raise Postgrex.Error, fn ->
        SearchOperations.do_query(base_so)
      end
    end

    test "do_query/1 renames count to value for aggregates and process_query_result/1 adds datetime",
         %{
           backend: backend,
           base_so: base_so
         } do
      unix_timestamp = DateTime.to_unix(~U[2026-01-29 05:13:48.748909Z], :microsecond)

      Backends
      |> expect(:get_default_backend, fn _user -> backend end)

      PostgresAdaptor
      |> expect(:execute_query, fn ^backend, %Ecto.Query{}, [query_type: :search] ->
        rows = [%{"count" => 2, "timestamp" => unix_timestamp}]
        {:ok, QueryResult.new(rows, %{total_rows: length(rows)})}
      end)

      result_so =
        %{base_so | type: :aggregates}
        |> SearchOperations.do_query()
        |> SearchOperations.process_query_result()

      assert result_so.rows == [
               %{
                 "value" => 2,
                 "timestamp" => unix_timestamp,
                 "datetime" => Timex.from_unix(unix_timestamp, :microsecond)
               }
             ]
    end
  end

  describe "put_chart_data_shape_id/1" do
    setup %{user: user} do
      source = insert(:source, user: user, bq_table_id: "test_table")

      insert(:source_schema,
        source: source,
        bigquery_schema:
          TestUtils.build_bq_schema(%{"metadata" => %{"response" => %{"status_code" => 200}}})
      )

      base_so = %SO{
        source: source,
        querystring: "",
        chart_data_shape_id: nil,
        tailing?: false,
        partition_by: :timestamp,
        type: :aggregates
      }

      [base_so: base_so]
    end

    test "sets custom data shape for count timestamp charts", %{base_so: base_so} do
      so = %{
        base_so
        | chart_rules: [
            %ChartRule{
              path: "timestamp",
              aggregate: :count,
              period: :minute,
              value_type: :datetime
            }
          ]
      }

      assert SearchOperations.put_chart_data_shape_id(so).chart_data_shape_id ==
               :cloudflare_status_codes
    end

    test "nil custom data shape for non-count metric charts", %{base_so: base_so} do
      so = %{
        base_so
        | chart_rules: [
            %ChartRule{
              path: "metadata.response.origin_time",
              aggregate: :p99,
              period: :minute,
              value_type: :integer
            }
          ]
      }

      assert SearchOperations.put_chart_data_shape_id(so).chart_data_shape_id == nil
    end
  end

  describe "apply_select_rules/1" do
    setup %{user: user} do
      source = insert(:source, user: user, bq_table_id: "test_table")

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
      source =
        so.source
        |> Ecto.Changeset.change(suggested_keys: "m.request_id, metadata.user_id")
        |> Logflare.Repo.update!()

      # Create schema with the suggested key fields as top-level fields
      insert(:source_schema,
        source: source,
        bigquery_schema:
          TestUtils.build_bq_schema(%{
            "metadata" => %{"request_id" => "req123", "user_id" => 123}
          })
      )

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
      source =
        so.source
        |> Ecto.Changeset.change(suggested_keys: "project!")
        |> Logflare.Repo.update!()

      insert(:source_schema,
        source: source,
        bigquery_schema: TestUtils.build_bq_schema(%{"project" => "my-project"})
      )

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
        SearchOperations.Helpers.get_min_max_filter_timestamps(filters, :minute)

      assert min_ts == Timex.shift(ts, minutes: -1)
      assert max_ts == Timex.shift(ts, minutes: 1)
    end

    test "returns unbounded interval message for open timestamp", %{
      ts_filters: ts_filters
    } do
      filters = [%{hd(ts_filters) | operator: :>}]

      %{message: message} =
        SearchOperations.Helpers.get_min_max_filter_timestamps(filters, :hour)

      assert message =~ "number of chart ticks is limited"
    end
  end

  describe "backend adaptor integration" do
    setup %{user: user} do
      source = insert(:source, user: user, bq_table_id: "test_table")

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
         QueryResult.new([%{"test" => "data"}], %{
           total_rows: 1,
           query_string: "",
           bq_params: []
         })}
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
