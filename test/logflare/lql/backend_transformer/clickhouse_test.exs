defmodule Logflare.Lql.BackendTransformer.ClickHouseTest do
  use ExUnit.Case, async: true

  import Ecto.Query

  alias Ecto.Query.DynamicExpr
  alias Logflare.Lql.BackendTransformer.ClickHouse
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Lql.Rules.SelectRule

  describe "behaviour implementation" do
    test "dialect/0 returns 'clickhouse'" do
      assert ClickHouse.dialect() == "clickhouse"
    end

    test "quote_style/0 returns double quotes" do
      assert ClickHouse.quote_style() == "\""
    end

    test "validate_transformation_data/1 requires schema" do
      assert ClickHouse.validate_transformation_data(%{schema: %{}}) == :ok

      assert ClickHouse.validate_transformation_data(%{}) ==
               {:error, "ClickHouse transformer requires schema in transformation data"}
    end

    test "build_transformation_data/1 returns base data as-is" do
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
