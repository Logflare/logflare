defmodule Logflare.Lql.BackendTransformer.PostgresTest do
  use ExUnit.Case, async: true

  import Ecto.Query

  alias Ecto.Query.DynamicExpr
  alias Logflare.Lql.BackendTransformer.Postgres
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Lql.Rules.SelectRule

  describe "behaviour implementation" do
    test "implements all required callbacks" do
      assert function_exported?(Postgres, :transform_filter_rule, 2)
      assert function_exported?(Postgres, :transform_chart_rule, 5)
      assert function_exported?(Postgres, :transform_select_rule, 2)
      assert function_exported?(Postgres, :apply_filter_rules_to_query, 3)
      assert function_exported?(Postgres, :dialect, 0)
      assert function_exported?(Postgres, :quote_style, 0)
      assert function_exported?(Postgres, :validate_transformation_data, 1)
      assert function_exported?(Postgres, :build_transformation_data, 1)
      assert function_exported?(Postgres, :handle_nested_field_access, 2)
    end

    test "returns correct dialect and quote style" do
      assert Postgres.dialect() == "postgres"
      assert Postgres.quote_style() == "\""
    end
  end

  describe "validate_transformation_data/1" do
    test "validates valid transformation data" do
      assert Postgres.validate_transformation_data(%{schema: %{}}) == :ok
    end

    test "rejects invalid transformation data" do
      assert Postgres.validate_transformation_data(%{}) ==
               {:error, "Postgres transformer requires schema in transformation data"}
    end
  end

  describe "build_transformation_data/1" do
    test "passes through base data as-is" do
      data = %{test: "value"}
      assert Postgres.build_transformation_data(data) == data
    end
  end

  describe "transform_filter_rule/2" do
    test "transforms equality filter on top-level field" do
      filter_rule =
        FilterRule.build(
          path: "event_message",
          operator: :=,
          value: "error",
          modifiers: %{}
        )

      result = Postgres.transform_filter_rule(filter_rule, %{})
      assert %DynamicExpr{} = result
    end

    test "transforms equality filter on nested JSONB field" do
      filter_rule =
        FilterRule.build(
          path: "m.status",
          operator: :=,
          value: "error",
          modifiers: %{}
        )

      result = Postgres.transform_filter_rule(filter_rule, %{})
      assert %DynamicExpr{} = result

      query = from(l in "logs")
      query_with_filter = where(query, ^result)
      assert %Ecto.Query{wheres: [_where_clause]} = query_with_filter
    end

    test "transforms regex filter with PostgreSQL ~ operator" do
      filter_rule =
        FilterRule.build(
          path: "event_message",
          operator: :"~",
          value: "server.*error",
          modifiers: %{}
        )

      result = Postgres.transform_filter_rule(filter_rule, %{})
      assert %DynamicExpr{} = result
    end

    test "transforms regex filter on JSONB field" do
      filter_rule =
        FilterRule.build(
          path: "m.message",
          operator: :"~",
          value: "server.*error",
          modifiers: %{}
        )

      result = Postgres.transform_filter_rule(filter_rule, %{})
      assert %DynamicExpr{} = result

      query = from(l in "logs")
      query_with_filter = where(query, ^result)
      assert %Ecto.Query{wheres: [_where_clause]} = query_with_filter
    end

    test "transforms string_contains filter" do
      filter_rule =
        FilterRule.build(
          path: "event_message",
          operator: :string_contains,
          value: "error",
          modifiers: %{}
        )

      result = Postgres.transform_filter_rule(filter_rule, %{})
      assert %DynamicExpr{} = result
    end

    test "transforms numeric comparison with type casting on JSONB" do
      filter_rule =
        FilterRule.build(
          path: "m.latency",
          operator: :>,
          value: 100,
          modifiers: %{}
        )

      result = Postgres.transform_filter_rule(filter_rule, %{})
      assert %DynamicExpr{} = result
    end

    test "transforms list_includes with JSONB @> operator" do
      filter_rule =
        FilterRule.build(
          path: "m.tags",
          operator: :list_includes,
          value: "production",
          modifiers: %{}
        )

      result = Postgres.transform_filter_rule(filter_rule, %{})
      assert %DynamicExpr{} = result
    end

    test "transforms range operator on top-level field" do
      filter_rule =
        FilterRule.build(
          path: "timestamp",
          operator: :range,
          values: [~U[2024-01-01 00:00:00Z], ~U[2024-01-02 00:00:00Z]],
          modifiers: %{}
        )

      result = Postgres.transform_filter_rule(filter_rule, %{})
      assert %DynamicExpr{} = result
    end

    test "transforms range operator on JSONB field" do
      filter_rule =
        FilterRule.build(
          path: "m.count",
          operator: :range,
          values: [1, 100],
          modifiers: %{}
        )

      result = Postgres.transform_filter_rule(filter_rule, %{})
      assert %DynamicExpr{} = result
    end

    test "transforms negated filter" do
      filter_rule =
        FilterRule.build(
          path: "event_message",
          operator: :=,
          value: "error",
          modifiers: %{negate: true}
        )

      result = Postgres.transform_filter_rule(filter_rule, %{})
      assert %DynamicExpr{} = result
    end
  end

  describe "apply_filter_rules_to_query/3" do
    test "applies multiple filter rules" do
      query = from(l in "logs")

      rules = [
        FilterRule.build(
          path: "event_message",
          operator: :string_contains,
          value: "error",
          modifiers: %{}
        ),
        FilterRule.build(path: "m.status", operator: :=, value: 500, modifiers: %{})
      ]

      result = Postgres.apply_filter_rules_to_query(query, rules, [])

      assert %Ecto.Query{} = result
      assert length(result.wheres) == 2
    end

    test "returns query unchanged when no rules" do
      query = from(l in "logs")
      result = Postgres.apply_filter_rules_to_query(query, [], [])
      assert result == query
    end
  end

  describe "transform_chart_rule/5" do
    test "generates count aggregation by second" do
      query = from(l in "logs")

      result =
        Postgres.transform_chart_rule(
          query,
          :count,
          "timestamp",
          :second,
          "timestamp"
        )

      assert %Ecto.Query{} = result
      assert result.group_bys != []
      assert result.order_bys != []
      assert result.select != nil
    end

    test "generates count aggregation by minute" do
      query = from(l in "logs")

      result =
        Postgres.transform_chart_rule(
          query,
          :count,
          "timestamp",
          :minute,
          "timestamp"
        )

      assert %Ecto.Query{} = result
      assert result.group_bys != []
      assert result.order_bys != []
    end

    test "generates count aggregation by hour" do
      query = from(l in "logs")

      result =
        Postgres.transform_chart_rule(
          query,
          :count,
          "timestamp",
          :hour,
          "timestamp"
        )

      assert %Ecto.Query{} = result
    end

    test "generates count aggregation by day" do
      query = from(l in "logs")

      result =
        Postgres.transform_chart_rule(
          query,
          :count,
          "timestamp",
          :day,
          "timestamp"
        )

      assert %Ecto.Query{} = result
    end

    test "generates avg aggregation on JSONB field" do
      query = from(l in "logs")

      result =
        Postgres.transform_chart_rule(
          query,
          :avg,
          "m.latency",
          :minute,
          "timestamp"
        )

      assert %Ecto.Query{} = result
    end

    test "generates sum aggregation on JSONB field" do
      query = from(l in "logs")

      result =
        Postgres.transform_chart_rule(
          query,
          :sum,
          "m.bytes",
          :hour,
          "timestamp"
        )

      assert %Ecto.Query{} = result
    end

    test "generates max aggregation on JSONB field" do
      query = from(l in "logs")

      result =
        Postgres.transform_chart_rule(
          query,
          :max,
          "m.response_time",
          :minute,
          "timestamp"
        )

      assert %Ecto.Query{} = result
    end

    test "generates percentile aggregation (p50)" do
      query = from(l in "logs")

      result =
        Postgres.transform_chart_rule(
          query,
          :p50,
          "m.response_time",
          :minute,
          "timestamp"
        )

      assert %Ecto.Query{} = result
    end

    test "generates percentile aggregation (p95)" do
      query = from(l in "logs")

      result =
        Postgres.transform_chart_rule(
          query,
          :p95,
          "m.response_time",
          :minute,
          "timestamp"
        )

      assert %Ecto.Query{} = result
    end

    test "generates percentile aggregation (p99)" do
      query = from(l in "logs")

      result =
        Postgres.transform_chart_rule(
          query,
          :p99,
          "m.latency",
          :hour,
          "timestamp"
        )

      assert %Ecto.Query{} = result
    end
  end

  describe "transform_select_rule/2" do
    test "transforms wildcard select" do
      select_rule = %SelectRule{wildcard: true, path: "*"}
      result = Postgres.transform_select_rule(select_rule, %{})
      assert result == {:wildcard, []}
    end

    test "transforms top-level field select" do
      select_rule = %SelectRule{wildcard: false, path: "event_message"}
      result = Postgres.transform_select_rule(select_rule, %{})
      assert result == {:field, "event_message", []}
    end

    test "transforms nested field select" do
      select_rule = %SelectRule{wildcard: false, path: "m.status"}
      result = Postgres.transform_select_rule(select_rule, %{})
      assert result == {:nested_field, "m.status", []}
    end
  end

  describe "apply_select_rules_to_query/3" do
    test "returns query unchanged for wildcard selection" do
      query = from(l in "logs")
      select_rules = [%SelectRule{wildcard: true, path: "*"}]

      result = Postgres.apply_select_rules_to_query(query, select_rules, [])
      assert result == query
    end

    test "returns query unchanged for empty select rules" do
      query = from(l in "logs")
      result = Postgres.apply_select_rules_to_query(query, [], [])
      assert result == query
    end

    test "builds combined select for specific fields" do
      query = from(l in "logs")

      select_rules = [
        %SelectRule{wildcard: false, path: "event_message"},
        %SelectRule{wildcard: false, path: "timestamp"}
      ]

      result = Postgres.apply_select_rules_to_query(query, select_rules, [])
      assert %Ecto.Query{} = result
      assert result.select != nil
    end
  end

  describe "handle_nested_field_access/2" do
    test "returns query unchanged (no joins needed for JSONB)" do
      query = from(l in "logs")
      result = Postgres.handle_nested_field_access(query, "m.status")
      assert result == query
    end
  end
end
