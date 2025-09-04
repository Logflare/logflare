defmodule Logflare.LqlTest do
  use Logflare.DataCase, async: true

  import Ecto.Query

  alias Logflare.Lql
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Lql.Rules.SelectRule
  alias Logflare.Lql.Rules.ChartRule
  alias Logflare.Sources.Source.BigQuery.SchemaBuilder

  describe "apply_filter_rules/3" do
    test "applies filter rules to query using BigQuery backend transformer by default" do
      query = from("test_table")

      filter_rule = %FilterRule{
        path: "metadata.status",
        operator: :=,
        value: "error",
        modifiers: %{}
      }

      result = Lql.apply_filter_rules(query, [filter_rule])
      assert %Ecto.Query{} = result
    end

    test "applies filter rules with custom adapter option" do
      query = from("test_table")

      filter_rule = %FilterRule{
        path: "metadata.status",
        operator: :=,
        value: "error",
        modifiers: %{}
      }

      result = Lql.apply_filter_rules(query, [filter_rule], adapter: :bigquery)
      assert %Ecto.Query{} = result
    end

    test "handles empty filter rules list" do
      query = from("test_table")

      result = Lql.apply_filter_rules(query, [])
      assert result == query
    end
  end

  describe "handle_nested_field_access/3" do
    test "handles nested field access using BigQuery backend transformer by default" do
      query = from("test_table")

      result = Lql.handle_nested_field_access(query, "metadata.user.id")
      assert %Ecto.Query{} = result
    end

    test "handles nested field access with custom adapter option" do
      query = from("test_table")

      result = Lql.handle_nested_field_access(query, "metadata.user.id", adapter: :bigquery)
      assert %Ecto.Query{} = result
    end
  end

  describe "transform_filter_rule/2" do
    test "transforms filter rule using BigQuery backend transformer by default" do
      filter_rule = %FilterRule{
        path: "metadata.status",
        operator: :=,
        value: "error",
        modifiers: %{}
      }

      result = Lql.transform_filter_rule(filter_rule)
      assert %Ecto.Query.DynamicExpr{} = result
    end

    test "transforms filter rule with custom adapter option" do
      filter_rule = %FilterRule{
        path: "metadata.status",
        operator: :=,
        value: "error",
        modifiers: %{}
      }

      result = Lql.transform_filter_rule(filter_rule, adapter: :bigquery)
      assert %Ecto.Query.DynamicExpr{} = result
    end
  end

  describe "decode/2" do
    test "works with BigQuery TableSchema" do
      lql_string = "m.status:error"
      schema = build_basic_schema()

      {:ok, rules} = Lql.decode(lql_string, schema)

      assert length(rules) == 1
      assert [%FilterRule{path: "metadata.status", operator: :=, value: "error"}] = rules
    end

    test "handles empty string with any schema" do
      {:ok, rules} = Lql.decode("", "any_schema")

      assert rules == []
    end
  end

  describe "decode!/2" do
    test "works with BigQuery TableSchema and returns rules directly" do
      lql_string = "m.status:error"
      schema = build_basic_schema()

      rules = Lql.decode!(lql_string, schema)

      assert length(rules) == 1
      assert [%FilterRule{path: "metadata.status", operator: :=, value: "error"}] = rules
    end

    test "handles empty string with any schema and returns rules directly" do
      rules = Lql.decode!("", "any_schema")

      assert rules == []
    end
  end

  describe "encode/1" do
    test "encodes filter rules to query string" do
      rules = [%FilterRule{path: "metadata.status", operator: :=, value: "error", modifiers: %{}}]

      {:ok, encoded} = Lql.encode(rules)

      assert encoded == "m.status:error"
    end

    test "handles empty rules list" do
      {:ok, encoded} = Lql.encode([])

      assert encoded == ""
    end
  end

  describe "encode!/1" do
    test "encodes filter rules to query string directly" do
      rules = [%FilterRule{path: "metadata.status", operator: :=, value: "error", modifiers: %{}}]

      encoded = Lql.encode!(rules)

      assert encoded == "m.status:error"
    end

    test "handles empty rules list" do
      encoded = Lql.encode!([])

      assert encoded == ""
    end
  end

  describe "decode/2 with message filters (legacy build_message_filter_from_regex behavior)" do
    test "builds filter from simple text" do
      text = "error"

      {:ok, rules} = Lql.decode(text, SchemaBuilder.initial_table_schema())

      assert length(rules) == 1
      assert [%FilterRule{path: "event_message", value: "error"}] = rules
    end

    test "builds filter from multiple words" do
      text = "database connection failed"

      {:ok, rules} = Lql.decode(text, SchemaBuilder.initial_table_schema())

      assert length(rules) == 3
      paths = Enum.map(rules, & &1.path)
      assert "event_message" in paths
    end

    test "builds filter from quoted string" do
      quoted_text = ~s|"user authentication failed"|

      {:ok, rules} = Lql.decode(quoted_text, SchemaBuilder.initial_table_schema())

      assert length(rules) == 1
      filter_rule = hd(rules)
      assert filter_rule.path == "event_message"
      assert filter_rule.value == "user authentication failed"
      assert filter_rule.modifiers.quoted_string == true
    end

    test "builds filter from regex pattern" do
      regex = ~s|~"error.*timeout"|

      {:ok, rules} = Lql.decode(regex, SchemaBuilder.initial_table_schema())

      assert length(rules) == 1
      filter_rule = hd(rules)
      assert filter_rule.path == "event_message"
      assert filter_rule.value == "error.*timeout"
      assert filter_rule.operator == :"~"
      assert filter_rule.modifiers.quoted_string == true
    end
  end

  defp build_basic_schema do
    %GoogleApi.BigQuery.V2.Model.TableSchema{
      fields: [
        %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
          name: "metadata",
          type: "RECORD",
          fields: [
            %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
              name: "status",
              type: "STRING"
            }
          ]
        }
      ]
    }
  end

  describe "apply_rules/3" do
    test "applies both filter and select rules to query" do
      query = from("test_table")

      filter_rule = %FilterRule{
        path: "event_message",
        operator: :=,
        value: "error",
        modifiers: %{}
      }

      select_rule = %SelectRule{
        path: "timestamp",
        wildcard: false
      }

      lql_rules = [filter_rule, select_rule]
      result_query = Lql.apply_rules(query, lql_rules)

      assert %Ecto.Query{} = result_query
      refute result_query == query
    end

    test "handles mixed rule types correctly" do
      query = from("test_table")

      lql_rules = [
        %FilterRule{path: "metadata.level", operator: :=, value: "info", modifiers: %{}},
        %SelectRule{path: "*", wildcard: true}
      ]

      result_query = Lql.apply_rules(query, lql_rules)

      assert %Ecto.Query{} = result_query
    end

    test "works with empty rule list" do
      query = from("test_table")

      result_query = Lql.apply_rules(query, [])

      assert result_query == query
    end
  end

  describe "chart rule parsing" do
    alias Logflare.Lql.Parser

    test "parses chart aggregations correctly" do
      test_cases = [
        {"c:count(host)", "host", :count, :minute},
        {"c:group_by(t::minute)", "timestamp", :count, :minute},
        {"c:count(host) c:group_by(t::minute)", "host", :count, :minute},
        {"c:avg(m.latency) c:group_by(t::hour)", "metadata.latency", :avg, :hour},
        {"c:sum(my_field.nested.value)", "my_field.nested.value", :sum, :minute},
        {"c:max(request.headers.content_length) c:group_by(t::day)",
         "request.headers.content_length", :max, :day}
      ]

      for {query, path, aggregate, period} <- test_cases do
        {:ok, [rule]} = Parser.parse(query)
        assert %ChartRule{path: ^path, aggregate: ^aggregate, period: ^period} = rule
      end
    end
  end
end
