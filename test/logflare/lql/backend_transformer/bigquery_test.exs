defmodule Logflare.Lql.BackendTransformer.BigQueryTest do
  use Logflare.DataCase, async: true

  import Ecto.Query

  alias Logflare.Lql.BackendTransformer.BigQuery
  alias Logflare.Lql.Rules.ChartRule
  alias Logflare.Lql.Rules.FilterRule

  @bq_table_id "test-table"

  describe "behavior implementation" do
    test "implements all required callbacks" do
      assert function_exported?(BigQuery, :transform_filter_rule, 2)
      assert function_exported?(BigQuery, :transform_chart_rule, 2)
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

      # Should add a WHERE clause for the top-level field
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

      # Should add both JOIN and WHERE clauses for nested fields
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

      # Should have both top-level WHERE and nested JOIN+WHERE
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

      # Should add a BETWEEN clause
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

      # Should add an INNER JOIN with UNNEST
      assert %Ecto.Query{joins: [join_clause]} = result
      assert join_clause.qual == :inner
    end

    test "adds multiple UNNEST joins for deeply nested fields" do
      query = from(@bq_table_id, select: [:timestamp, :metadata])

      result =
        BigQuery.handle_nested_field_access(query, "metadata.request.headers.authorization")

      # Should add multiple INNER JOINs with UNNEST
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

  describe "transform_chart_rule/2" do
    test "raises error for unimplemented chart rule transformation" do
      chart_rule = %ChartRule{
        path: "timestamp",
        aggregate: :count,
        period: :minute,
        value_type: :integer
      }

      assert_raise RuntimeError, ~r/Chart rule transformation not yet implemented/, fn ->
        BigQuery.transform_chart_rule(chart_rule, %{})
      end
    end
  end
end
