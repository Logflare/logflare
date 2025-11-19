defmodule Logflare.Lql.BackendTransformer.BigQueryTest do
  use Logflare.DataCase, async: true

  import Ecto.Query

  alias Ecto.Query
  alias Ecto.Query.BooleanExpr
  alias Logflare.Lql.BackendTransformer.BigQuery
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Lql.Rules.SelectRule

  @bq_table_id "test-table"

  describe "behavior implementation" do
    test "implements all required callbacks" do
      assert function_exported?(BigQuery, :transform_filter_rule, 2)
      assert function_exported?(BigQuery, :transform_chart_rule, 5)
      assert function_exported?(BigQuery, :transform_select_rule, 2)
      assert function_exported?(BigQuery, :apply_filter_rules_to_query, 3)
      assert function_exported?(BigQuery, :dialect, 0)
      assert function_exported?(BigQuery, :quote_style, 0)
      assert function_exported?(BigQuery, :validate_transformation_data, 1)
      assert function_exported?(BigQuery, :build_transformation_data, 1)
      assert function_exported?(BigQuery, :handle_nested_field_access, 2)
    end

    test "returns correct dialect and quote style" do
      assert BigQuery.dialect() == "bigquery"
      assert BigQuery.quote_style() == "`"
    end
  end

  describe "validate_transformation_data/1" do
    test "validates valid transformation data" do
      valid_data = %{schema: %{}}
      assert BigQuery.validate_transformation_data(valid_data) == :ok
    end

    test "rejects invalid transformation data" do
      invalid_data = %{}
      assert {:error, message} = BigQuery.validate_transformation_data(invalid_data)
      assert message =~ "BigQuery transformer requires schema"
    end
  end

  describe "build_transformation_data/1" do
    test "passes through base data as-is" do
      base_data = %{sources: [], schema: %{}}
      result = BigQuery.build_transformation_data(base_data)
      assert result == base_data
    end
  end

  describe "apply_filter_rules_to_query/3" do
    test "returns query unchanged when no filter rules" do
      query = from(@bq_table_id, select: [:timestamp])
      result = BigQuery.apply_filter_rules_to_query(query, [])
      assert result == query
    end

    test "applies top-level filter rules" do
      query = from(@bq_table_id, select: [:timestamp, :event_message])

      filter_rule = %FilterRule{
        path: "event_message",
        operator: :=,
        value: "test message",
        modifiers: %{}
      }

      result = BigQuery.apply_filter_rules_to_query(query, [filter_rule])

      assert %Ecto.Query{wheres: [_where_clause]} = result
    end

    test "applies nested filter rules with UNNEST" do
      query = from(@bq_table_id, select: [:timestamp, :metadata])

      filter_rule = %FilterRule{
        path: "metadata.status",
        operator: :=,
        value: "success",
        modifiers: %{}
      }

      result = BigQuery.apply_filter_rules_to_query(query, [filter_rule])

      assert %Ecto.Query{joins: [_join_clause], wheres: [_where_clause]} = result
    end

    test "handles multiple filter rules" do
      query = from(@bq_table_id, select: [:timestamp, :metadata])

      filter_rules = [
        %FilterRule{
          path: "event_message",
          operator: :=,
          value: "error",
          modifiers: %{}
        },
        %FilterRule{
          path: "metadata.status",
          operator: :=,
          value: "500",
          modifiers: %{}
        }
      ]

      result = BigQuery.apply_filter_rules_to_query(query, filter_rules)

      assert %Ecto.Query{joins: [_join_clause], wheres: [_where1, _where2]} = result
    end

    test "handles range operator" do
      query = from(@bq_table_id, select: [:timestamp])

      filter_rule = %FilterRule{
        path: "timestamp",
        operator: :range,
        values: [~N[2023-01-01 00:00:00], ~N[2023-01-02 00:00:00]],
        modifiers: %{}
      }

      result = BigQuery.apply_filter_rules_to_query(query, [filter_rule])

      assert %Ecto.Query{wheres: [_where_clause]} = result
    end
  end

  describe "handle_nested_field_access/2" do
    test "returns query unchanged for top-level fields" do
      query = from(@bq_table_id, select: [:timestamp])
      result = BigQuery.handle_nested_field_access(query, "timestamp")
      assert result == query
    end

    test "adds UNNEST joins for nested fields" do
      query = from(@bq_table_id, select: [:timestamp, :metadata])
      result = BigQuery.handle_nested_field_access(query, "metadata.status")

      assert %Ecto.Query{joins: [join_clause]} = result
      assert join_clause.qual == :inner
    end

    test "adds multiple UNNEST joins for deeply nested fields" do
      query = from(@bq_table_id, select: [:timestamp, :metadata])

      result =
        BigQuery.handle_nested_field_access(query, "metadata.request.headers.authorization")

      assert %Ecto.Query{joins: [_join1, _join2, _join3]} = result
    end
  end

  describe "transform_filter_rule/2" do
    test "transforms simple equality filter" do
      filter_rule = %FilterRule{
        path: "status",
        operator: :=,
        value: "success",
        modifiers: %{}
      }

      result = BigQuery.transform_filter_rule(filter_rule, %{})
      assert %Ecto.Query.DynamicExpr{} = result
    end

    test "transforms regex filter" do
      filter_rule = %FilterRule{
        path: "message",
        operator: :"~",
        value: "error.*",
        modifiers: %{}
      }

      result = BigQuery.transform_filter_rule(filter_rule, %{})
      assert %Ecto.Query.DynamicExpr{} = result
    end

    test "transforms negated filter" do
      filter_rule = %FilterRule{
        path: "status",
        operator: :=,
        value: "error",
        modifiers: %{negate: true}
      }

      result = BigQuery.transform_filter_rule(filter_rule, %{})
      assert %Ecto.Query.DynamicExpr{} = result
    end

    test "transforms range filter" do
      filter_rule = %FilterRule{
        path: "timestamp",
        operator: :range,
        values: [~N[2023-01-01 00:00:00], ~N[2023-01-02 00:00:00]],
        modifiers: %{}
      }

      result = BigQuery.transform_filter_rule(filter_rule, %{})
      assert %Ecto.Query.DynamicExpr{} = result
    end

    test "transforms list_includes filter" do
      filter_rule = %FilterRule{
        path: "tags",
        operator: :list_includes,
        value: "production",
        modifiers: %{}
      }

      result = BigQuery.transform_filter_rule(filter_rule, %{})
      assert %Ecto.Query.DynamicExpr{} = result
    end

    test "transforms string_contains filter" do
      filter_rule = %FilterRule{
        path: "message",
        operator: :string_contains,
        value: "error",
        modifiers: %{}
      }

      result = BigQuery.transform_filter_rule(filter_rule, %{})
      assert %Ecto.Query.DynamicExpr{} = result
    end

    test "transforms NULL check filter" do
      filter_rule = %FilterRule{
        path: "user_id",
        operator: :=,
        value: :NULL,
        modifiers: %{}
      }

      result = BigQuery.transform_filter_rule(filter_rule, %{})
      assert %Ecto.Query.DynamicExpr{} = result
    end
  end

  describe "transform_chart_rule/5" do
    setup do
      base_query = from(@bq_table_id)
      [base_query: base_query]
    end

    test "transforms count aggregation with minute period", %{base_query: base_query} do
      result = BigQuery.transform_chart_rule(base_query, :count, "*", :minute, "timestamp")

      assert %Query{} = result
      assert result.group_bys != []
      assert result.order_bys != []
    end

    test "transforms avg aggregation with hour period", %{base_query: base_query} do
      result = BigQuery.transform_chart_rule(base_query, :avg, "latency", :hour, "timestamp")

      assert %Query{} = result
      assert result.group_bys != []
    end

    test "transforms sum aggregation with day period", %{base_query: base_query} do
      result = BigQuery.transform_chart_rule(base_query, :sum, "bytes", :day, "timestamp")

      assert %Query{} = result
      assert result.group_bys != []
    end

    test "transforms max aggregation with second period", %{base_query: base_query} do
      result =
        BigQuery.transform_chart_rule(base_query, :max, "response_time", :second, "timestamp")

      assert %Query{} = result
      assert result.group_bys != []
    end

    test "transforms p50 percentile aggregation", %{base_query: base_query} do
      result = BigQuery.transform_chart_rule(base_query, :p50, "duration", :minute, "timestamp")

      assert %Query{} = result
      assert result.group_bys != []
    end

    test "transforms p95 percentile aggregation", %{base_query: base_query} do
      result = BigQuery.transform_chart_rule(base_query, :p95, "latency", :hour, "timestamp")

      assert %Query{} = result
      assert result.group_bys != []
    end

    test "transforms p99 percentile aggregation", %{base_query: base_query} do
      result = BigQuery.transform_chart_rule(base_query, :p99, "latency", :day, "timestamp")

      assert %Query{} = result
      assert result.group_bys != []
    end
  end

  describe "transform_select_rule/2" do
    test "transforms wildcard select rule" do
      select_rule = %SelectRule{path: "*", wildcard: true}

      result = BigQuery.transform_select_rule(select_rule, %{})

      assert result == {:wildcard, []}
    end

    test "transforms top-level field select rule" do
      select_rule = %SelectRule{path: "event_message", wildcard: false}

      result = BigQuery.transform_select_rule(select_rule, %{})

      assert result == {:field, "event_message", []}
    end

    test "transforms nested field select rule" do
      select_rule = %SelectRule{path: "metadata.user.id", wildcard: false}

      result = BigQuery.transform_select_rule(select_rule, %{})

      assert {:nested_field, ["metadata", "user", "id"], ["metadata", "metadata.user"]} = result
    end

    test "transforms special top-level field select rule" do
      select_rule = %SelectRule{path: "timestamp", wildcard: false}

      result = BigQuery.transform_select_rule(select_rule, %{})

      assert result == {:field, "timestamp", []}
    end
  end

  describe "apply_select_rules_to_query/3" do
    setup do
      base_query = from(l in @bq_table_id, select: l)
      [base_query: base_query]
    end

    test "returns query unchanged for empty select rules", %{base_query: base_query} do
      result = BigQuery.apply_select_rules_to_query(base_query, [], [])

      assert result == base_query
    end

    test "returns query unchanged for wildcard select rule", %{base_query: base_query} do
      select_rules = [%SelectRule{path: "*", wildcard: true}]

      result = BigQuery.apply_select_rules_to_query(base_query, select_rules, [])

      assert result == base_query
    end

    test "applies wildcard precedence - ignores other rules when wildcard present", %{
      base_query: base_query
    } do
      select_rules = [
        %SelectRule{path: "event_message", wildcard: false},
        %SelectRule{path: "*", wildcard: true},
        %SelectRule{path: "timestamp", wildcard: false}
      ]

      result = BigQuery.apply_select_rules_to_query(base_query, select_rules, [])

      assert result == base_query
    end

    test "applies single top-level field selection", %{base_query: base_query} do
      select_rules = [%SelectRule{path: "event_message", wildcard: false}]

      result = BigQuery.apply_select_rules_to_query(base_query, select_rules, [])

      assert %Query{select: %{expr: expr}} = result
      assert expr |> Macro.to_string() =~ "event_message"
      refute result == base_query
    end

    test "applies select rules without opts argument", %{base_query: base_query} do
      select_rules = [%SelectRule{path: "timestamp", wildcard: false}]
      result = BigQuery.apply_select_rules_to_query(base_query, select_rules)

      assert %Query{} = result
      refute result == base_query
    end

    test "handles nested field selection triggering dot replacement in build_combined_select", %{
      base_query: base_query
    } do
      select_rules = [%SelectRule{path: "user.profile.name", wildcard: false}]
      result = BigQuery.apply_select_rules_to_query(base_query, select_rules, [])

      assert %Query{select: %{expr: expr}} = result
      assert expr |> Macro.to_string() =~ "user_profile_name"
      refute result == base_query
    end

    test "handles empty normalized rules case", %{base_query: base_query} do
      select_rules = []

      result = BigQuery.apply_select_rules_to_query(base_query, select_rules, [])

      assert result == base_query
    end
  end

  describe "where_timestamp_ago/4" do
    setup do
      base_query = from("logs")
      test_datetime = ~U[2025-02-21 03:27:12Z]
      {:ok, base_query: base_query, datetime: test_datetime}
    end

    test "adds timestamp filter for MINUTE interval", %{
      base_query: base_query,
      datetime: datetime
    } do
      result = BigQuery.where_timestamp_ago(base_query, datetime, 5, "MINUTE")

      assert %Query{wheres: [%BooleanExpr{expr: expr}]} = result
      assert {:>=, _, [_, {:fragment, _, fragment_parts}]} = expr

      fragment_string =
        fragment_parts
        |> Enum.filter(&match?({:raw, _}, &1))
        |> Enum.map(fn {:raw, s} -> s end)
        |> Enum.join()

      assert fragment_string == "TIMESTAMP_SUB(, INTERVAL  MINUTE)"
    end

    test "adds timestamp filter for HOUR interval", %{base_query: base_query, datetime: datetime} do
      result = BigQuery.where_timestamp_ago(base_query, datetime, 24, "HOUR")

      assert %Query{wheres: [%BooleanExpr{expr: expr}]} = result
      assert {:>=, _, [_, {:fragment, _, fragment_parts}]} = expr

      fragment_string =
        fragment_parts
        |> Enum.filter(&match?({:raw, _}, &1))
        |> Enum.map(fn {:raw, s} -> s end)
        |> Enum.join()

      assert fragment_string == "TIMESTAMP_SUB(, INTERVAL  HOUR)"
    end

    test "adds timestamp filter for DAY interval", %{base_query: base_query, datetime: datetime} do
      result = BigQuery.where_timestamp_ago(base_query, datetime, 7, "DAY")

      assert %Query{wheres: [%BooleanExpr{expr: expr}]} = result
      assert {:>=, _, [_, {:fragment, _, fragment_parts}]} = expr

      fragment_string =
        fragment_parts
        |> Enum.filter(&match?({:raw, _}, &1))
        |> Enum.map(fn {:raw, s} -> s end)
        |> Enum.join()

      assert fragment_string == "TIMESTAMP_SUB(, INTERVAL  DAY)"
    end

    test "adds timestamp filter for SECOND interval", %{
      base_query: base_query,
      datetime: datetime
    } do
      result = BigQuery.where_timestamp_ago(base_query, datetime, 30, "SECOND")

      assert %Query{wheres: [%BooleanExpr{expr: expr}]} = result
      assert {:>=, _, [_, {:fragment, _, fragment_parts}]} = expr

      fragment_string =
        fragment_parts
        |> Enum.filter(&match?({:raw, _}, &1))
        |> Enum.map(fn {:raw, s} -> s end)
        |> Enum.join()

      assert fragment_string == "TIMESTAMP_SUB(, INTERVAL  SECOND)"
    end

    test "adds timestamp filter for MILLISECOND interval", %{
      base_query: base_query,
      datetime: datetime
    } do
      result = BigQuery.where_timestamp_ago(base_query, datetime, 1000, "MILLISECOND")

      assert %Query{wheres: [%BooleanExpr{expr: expr}]} = result
      assert {:>=, _, [_, {:fragment, _, fragment_parts}]} = expr

      fragment_string =
        fragment_parts
        |> Enum.filter(&match?({:raw, _}, &1))
        |> Enum.map(fn {:raw, s} -> s end)
        |> Enum.join()

      assert fragment_string == "TIMESTAMP_SUB(, INTERVAL  MILLISECOND)"
    end

    test "adds timestamp filter for MICROSECOND interval", %{
      base_query: base_query,
      datetime: datetime
    } do
      result = BigQuery.where_timestamp_ago(base_query, datetime, 1_000_000, "MICROSECOND")

      assert %Query{wheres: [%BooleanExpr{expr: expr}]} = result
      assert {:>=, _, [_, {:fragment, _, fragment_parts}]} = expr

      fragment_string =
        fragment_parts
        |> Enum.filter(&match?({:raw, _}, &1))
        |> Enum.map(fn {:raw, s} -> s end)
        |> Enum.join()

      assert fragment_string == "TIMESTAMP_SUB(, INTERVAL  MICROSECOND)"
    end

    test "raises ArgumentError for invalid interval", %{
      base_query: base_query,
      datetime: datetime
    } do
      assert_raise ArgumentError, "Invalid interval: INVALID", fn ->
        BigQuery.where_timestamp_ago(base_query, datetime, 1, "INVALID")
      end
    end

    test "composes with existing where clauses", %{datetime: datetime} do
      assert %Query{} =
               query =
               from("logs")
               |> where([t], t.level == "error")
               |> BigQuery.where_timestamp_ago(datetime, 10, "MINUTE")
               |> where([t], t.status == 500)

      assert length(query.wheres) == 3
    end

    test "all intervals generate valid queries", %{base_query: base_query, datetime: datetime} do
      intervals = [
        {"MICROSECOND", 1_000_000},
        {"MILLISECOND", 1000},
        {"SECOND", 60},
        {"MINUTE", 60},
        {"HOUR", 24},
        {"DAY", 7}
      ]

      for {unit, count} <- intervals do
        assert %Query{wheres: [%BooleanExpr{} | _]} =
                 BigQuery.where_timestamp_ago(base_query, datetime, count, unit)
      end
    end
  end
end
