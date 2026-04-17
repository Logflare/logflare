defmodule Logflare.Lql.BackendTransformer.ClickHouseTest do
  use Logflare.DataCase, async: true

  import Ecto.Query

  alias Ecto.Query.DynamicExpr
  alias Logflare.Lql.BackendTransformer.ClickHouse
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Lql.Rules.SelectRule

  describe "behaviour implementation" do
    test "implements all required callbacks" do
      assert function_exported?(ClickHouse, :transform_filter_rule, 2)
      assert function_exported?(ClickHouse, :transform_chart_rule, 5)
      assert function_exported?(ClickHouse, :transform_select_rule, 2)
      assert function_exported?(ClickHouse, :apply_filter_rules_to_query, 3)
      assert function_exported?(ClickHouse, :dialect, 0)
      assert function_exported?(ClickHouse, :quote_style, 0)
      assert function_exported?(ClickHouse, :validate_transformation_data, 1)
      assert function_exported?(ClickHouse, :build_transformation_data, 1)
      assert function_exported?(ClickHouse, :handle_nested_field_access, 2)
    end

    test "returns correct dialect and quote style" do
      assert ClickHouse.dialect() == "clickhouse"
      assert ClickHouse.quote_style() == "\""
    end
  end

  describe "validate_transformation_data/1" do
    test "validates valid transformation data" do
      assert ClickHouse.validate_transformation_data(%{schema: %{}}) == :ok
    end

    test "rejects invalid transformation data" do
      assert ClickHouse.validate_transformation_data(%{}) ==
               {:error, "ClickHouse transformer requires schema in transformation data"}
    end
  end

  describe "build_transformation_data/1" do
    test "passes through base data as-is" do
      data = %{test: "value"}
      assert ClickHouse.build_transformation_data(data) == data
    end
  end

  describe "transform_filter_rule/2" do
    test "transforms equality filter" do
      filter_rule =
        FilterRule.build(
          path: "event_message",
          operator: :=,
          value: "error",
          modifiers: %{}
        )

      result = ClickHouse.transform_filter_rule(filter_rule, %{})
      assert %DynamicExpr{} = result
    end

    test "transforms regex filter with ClickHouse match() function" do
      filter_rule =
        FilterRule.build(
          path: "event_message",
          operator: :"~",
          value: "server.*error",
          modifiers: %{}
        )

      result = ClickHouse.transform_filter_rule(filter_rule, %{})
      assert %DynamicExpr{} = result

      query = from(l in "logs")
      query_with_filter = where(query, ^result)
      assert %Ecto.Query{wheres: [_where_clause]} = query_with_filter
    end

    test "transforms string_contains filter with ClickHouse position() function" do
      filter_rule =
        FilterRule.build(
          path: "event_message",
          operator: :string_contains,
          value: "error",
          modifiers: %{}
        )

      result = ClickHouse.transform_filter_rule(filter_rule, %{})
      assert %DynamicExpr{} = result

      query = from(l in "logs")
      query_with_filter = where(query, ^result)
      assert %Ecto.Query{wheres: [_where_clause]} = query_with_filter
    end

    test "transforms list_includes filter with ClickHouse has() function" do
      filter_rule =
        FilterRule.build(
          path: "tags",
          operator: :list_includes,
          value: "production",
          modifiers: %{}
        )

      result = ClickHouse.transform_filter_rule(filter_rule, %{})
      assert %DynamicExpr{} = result

      query = from(l in "logs")
      query_with_filter = where(query, ^result)
      assert %Ecto.Query{wheres: [_where_clause]} = query_with_filter
    end

    test "transforms list_includes_regexp filter with ClickHouse arrayExists function" do
      filter_rule =
        FilterRule.build(
          path: "tags",
          operator: :list_includes_regexp,
          value: "prod.*",
          modifiers: %{}
        )

      result = ClickHouse.transform_filter_rule(filter_rule, %{})
      assert %DynamicExpr{} = result

      query = from(l in "logs")
      query_with_filter = where(query, ^result)
      assert %Ecto.Query{wheres: [_where_clause]} = query_with_filter
    end

    test "transforms range filter with BETWEEN clause" do
      filter_rule =
        FilterRule.build(
          path: "latency",
          operator: :range,
          values: [100, 500],
          modifiers: %{}
        )

      result = ClickHouse.transform_filter_rule(filter_rule, %{})
      assert %DynamicExpr{} = result

      query = from(l in "logs")
      query_with_filter = where(query, ^result)
      assert %Ecto.Query{wheres: [_where_clause]} = query_with_filter
    end

    test "transforms NULL value filter" do
      filter_rule =
        FilterRule.build(
          path: "optional_field",
          operator: :=,
          value: :NULL,
          modifiers: %{}
        )

      result = ClickHouse.transform_filter_rule(filter_rule, %{})
      assert %DynamicExpr{} = result

      query = from(l in "logs")
      query_with_filter = where(query, ^result)
      assert %Ecto.Query{wheres: [_where_clause]} = query_with_filter
    end

    test "transforms negated filter" do
      filter_rule =
        FilterRule.build(
          path: "event_message",
          operator: :=,
          value: "error",
          modifiers: %{negate: true}
        )

      result = ClickHouse.transform_filter_rule(filter_rule, %{})
      assert %DynamicExpr{} = result
    end
  end

  describe "split_map_path/1" do
    test "returns map_access for known Map column with dot key" do
      assert {:map_access, "log_attributes", "parsed.backend_type"} =
               ClickHouse.split_map_path("log_attributes.parsed.backend_type")
    end

    test "returns map_access for all known Map columns" do
      for col <-
            ~w(log_attributes resource_attributes scope_attributes attributes span_attributes) do
        assert {:map_access, ^col, "key"} = ClickHouse.split_map_path("#{col}.key")
      end
    end

    test "returns column for non-Map column" do
      assert {:column, "event_message"} = ClickHouse.split_map_path("event_message")
    end

    test "returns column for unknown dotted path" do
      assert {:column, "unknown_col.field"} = ClickHouse.split_map_path("unknown_col.field")
    end
  end

  describe "transform_filter_rule/2 with dot-key Map paths" do
    test "transforms equality filter on Map column dot-key" do
      filter_rule =
        FilterRule.build(
          path: "log_attributes.parsed.backend_type",
          operator: :=,
          value: "client",
          modifiers: %{}
        )

      result = ClickHouse.transform_filter_rule(filter_rule, %{})
      assert %DynamicExpr{} = result

      query = from(l in "logs")
      query_with_filter = where(query, ^result)
      assert %Ecto.Query{wheres: [_where_clause]} = query_with_filter
    end

    test "transforms range filter on Map column dot-key" do
      filter_rule =
        FilterRule.build(
          path: "log_attributes.response_time",
          operator: :range,
          values: [100, 500],
          modifiers: %{}
        )

      result = ClickHouse.transform_filter_rule(filter_rule, %{})
      assert %DynamicExpr{} = result

      query = from(l in "logs")
      query_with_filter = where(query, ^result)
      assert %Ecto.Query{wheres: [_where_clause]} = query_with_filter
    end

    test "transforms negated filter on Map column dot-key" do
      filter_rule =
        FilterRule.build(
          path: "log_attributes.parsed.backend_type",
          operator: :=,
          value: "client",
          modifiers: %{negate: true}
        )

      result = ClickHouse.transform_filter_rule(filter_rule, %{})
      assert %DynamicExpr{} = result

      query = from(l in "logs")
      query_with_filter = where(query, ^result)
      assert %Ecto.Query{wheres: [_where_clause]} = query_with_filter
    end
  end

  describe "apply_filter_rules_to_query/3 with dot-key Map paths" do
    test "applies range filter on Map column dot-key to query" do
      query = from(l in "logs")

      filter_rule =
        FilterRule.build(
          path: "log_attributes.response_time",
          operator: :range,
          values: [100, 500],
          modifiers: %{}
        )

      result = ClickHouse.apply_filter_rules_to_query(query, [filter_rule], [])
      assert %Ecto.Query{wheres: [_where_clause]} = result
    end
  end

  describe "apply_select_rules_to_query/3 with dot-key Map paths" do
    test "applies select rule for Map column dot-key" do
      query = from(l in "logs")
      select_rule = %SelectRule{path: "log_attributes.parsed.backend_type"}

      result = ClickHouse.apply_select_rules_to_query(query, [select_rule], [])

      assert %Ecto.Query{select: %{expr: expr}} = result
      assert Macro.to_string(expr) =~ "log_attributes_parsed_backend_type"
    end

    test "applies select rule for Map column dot-key with alias" do
      query = from(l in "logs")

      select_rule = %SelectRule{
        path: "log_attributes.parsed.backend_type",
        alias: "backend_type"
      }

      result = ClickHouse.apply_select_rules_to_query(query, [select_rule], [])

      assert %Ecto.Query{select: %{expr: expr}} = result
      assert Macro.to_string(expr) =~ "backend_type"
    end
  end

  describe "transform_chart_rule/5" do
    setup do
      base_query = from("logs")
      [base_query: base_query]
    end

    test "transforms count aggregation with minute period", %{base_query: base_query} do
      result = ClickHouse.transform_chart_rule(base_query, :count, "*", :minute, "timestamp")

      assert %Ecto.Query{} = result
      assert result.group_bys != []
      assert result.order_bys != []
    end

    test "transforms avg aggregation with hour period", %{base_query: base_query} do
      result = ClickHouse.transform_chart_rule(base_query, :avg, "latency", :hour, "timestamp")

      assert %Ecto.Query{} = result
      assert result.group_bys != []
    end

    test "transforms sum aggregation with day period", %{base_query: base_query} do
      result = ClickHouse.transform_chart_rule(base_query, :sum, "bytes", :day, "timestamp")

      assert %Ecto.Query{} = result
      assert result.group_bys != []
    end

    test "transforms max aggregation with second period", %{base_query: base_query} do
      result =
        ClickHouse.transform_chart_rule(base_query, :max, "response_time", :second, "timestamp")

      assert %Ecto.Query{} = result
      assert result.group_bys != []
    end

    test "transforms p50 percentile aggregation", %{base_query: base_query} do
      result = ClickHouse.transform_chart_rule(base_query, :p50, "duration", :minute, "timestamp")

      assert %Ecto.Query{} = result
      assert result.group_bys != []
    end

    test "transforms p95 percentile aggregation", %{base_query: base_query} do
      result = ClickHouse.transform_chart_rule(base_query, :p95, "latency", :hour, "timestamp")

      assert %Ecto.Query{} = result
      assert result.group_bys != []
    end

    test "transforms p99 percentile aggregation", %{base_query: base_query} do
      result = ClickHouse.transform_chart_rule(base_query, :p99, "latency", :day, "timestamp")

      assert %Ecto.Query{} = result
      assert result.group_bys != []
    end
  end

  describe "transform_select_rule/2" do
    test "transforms wildcard select rule" do
      select_rule = %{wildcard: true}

      result = ClickHouse.transform_select_rule(select_rule, %{})
      assert result == {:wildcard, []}
    end

    test "transforms field select rule for top-level field" do
      select_rule = %{path: "event_message", wildcard: false}

      result = ClickHouse.transform_select_rule(select_rule, %{})
      assert result == {:field, "event_message", []}
    end

    test "transforms field select rule for nested field" do
      select_rule = %{path: "metadata.user.id", wildcard: false}

      result = ClickHouse.transform_select_rule(select_rule, %{})
      assert result == {:nested_field, "metadata.user.id", []}
    end
  end

  describe "apply_filter_rules_to_query/3" do
    test "returns query unchanged when no rules" do
      query = from(l in "logs")
      result = ClickHouse.apply_filter_rules_to_query(query, [], [])
      assert result == query
    end

    test "applies single filter rule to query" do
      query = from(l in "logs")

      filter_rule =
        FilterRule.build(
          path: "event_message",
          operator: :=,
          value: "error",
          modifiers: %{}
        )

      result = ClickHouse.apply_filter_rules_to_query(query, [filter_rule], [])
      assert %Ecto.Query{wheres: [_where_clause]} = result
    end
  end

  describe "apply_select_rules_to_query/3" do
    test "returns query unchanged when no rules" do
      query = from(l in "logs")
      result = ClickHouse.apply_select_rules_to_query(query, [], [])
      assert result == query
    end

    test "returns query unchanged for wildcard selection" do
      query = from(l in "logs")
      select_rule = %SelectRule{path: "*", wildcard: true}

      result = ClickHouse.apply_select_rules_to_query(query, [select_rule], [])
      assert result == query
    end

    test "applies select rule to query" do
      query = from(l in "logs")
      select_rule = %SelectRule{path: "metadata.user.id"}

      result = ClickHouse.apply_select_rules_to_query(query, [select_rule], [])

      assert %Ecto.Query{select: %{expr: expr}} = result
      assert Macro.to_string(expr) =~ "metadata_user_id"
    end

    test "applies top-level field with alias" do
      query = from(l in "logs")
      select_rule = %SelectRule{path: "event_message", alias: "msg"}

      result = ClickHouse.apply_select_rules_to_query(query, [select_rule], [])

      assert %Ecto.Query{select: %{expr: expr}} = result
      assert Macro.to_string(expr) =~ "msg"
    end

    test "applies nested field with alias" do
      query = from(l in "logs")

      select_rule = %SelectRule{path: "metadata.user.id", alias: "user_id"}

      result = ClickHouse.apply_select_rules_to_query(query, [select_rule], [])

      assert %Ecto.Query{select: %{expr: expr}} = result
      assert Macro.to_string(expr) =~ "user_id"
    end
  end

  describe "handle_nested_field_access/2" do
    test "returns query unchanged for ClickHouse native nested field handling" do
      query = from(l in "logs")
      result = ClickHouse.handle_nested_field_access(query, "metadata.user.id")
      assert result == query
    end
  end

  describe "where_timestamp_ago/4" do
    test "handles MICROSECOND interval with ClickHouse function" do
      query = from(l in "logs")
      datetime = DateTime.utc_now()

      result = ClickHouse.where_timestamp_ago(query, datetime, 1000, "MICROSECOND")
      assert %Ecto.Query{wheres: [_where_clause]} = result
    end

    test "handles DAY interval with ClickHouse function" do
      query = from(l in "logs")
      datetime = DateTime.utc_now()

      result = ClickHouse.where_timestamp_ago(query, datetime, 7, "DAY")
      assert %Ecto.Query{wheres: [_where_clause]} = result
    end

    test "raises for invalid interval" do
      query = from(l in "logs")
      datetime = DateTime.utc_now()

      assert_raise ArgumentError, "Invalid interval: INVALID", fn ->
        ClickHouse.where_timestamp_ago(query, datetime, 1, "INVALID")
      end
    end
  end
end
