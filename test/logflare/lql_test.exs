defmodule Logflare.LqlTest do
  use Logflare.DataCase, async: true

  import Ecto.Query

  alias Logflare.Lql
  alias Logflare.Lql.Rules.FilterRule

  describe "apply_filter_rules_to_query/3" do
    test "applies filter rules to query using BigQuery backend transformer by default" do
      query = from("test_table")

      filter_rule = %FilterRule{
        path: "metadata.status",
        operator: :=,
        value: "error",
        modifiers: %{}
      }

      result = Lql.apply_filter_rules_to_query(query, [filter_rule])
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

      result = Lql.apply_filter_rules_to_query(query, [filter_rule], adapter: :bigquery)
      assert %Ecto.Query{} = result
    end

    test "handles empty filter rules list" do
      query = from("test_table")

      result = Lql.apply_filter_rules_to_query(query, [])
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

  describe "build_message_filter_from_regex/1" do
    test "builds filter from simple text" do
      text = "error"

      {:ok, rules} = Lql.build_message_filter_from_regex(text)

      assert length(rules) == 1
      assert [%FilterRule{path: "event_message", value: "error"}] = rules
    end

    test "builds filter from multiple words" do
      text = "database connection failed"

      {:ok, rules} = Lql.build_message_filter_from_regex(text)

      assert length(rules) == 3
      paths = Enum.map(rules, & &1.path)
      assert "event_message" in paths
    end

    test "builds filter from quoted string" do
      quoted_text = ~s|"user authentication failed"|

      {:ok, rules} = Lql.build_message_filter_from_regex(quoted_text)

      assert length(rules) == 1
      filter_rule = hd(rules)
      assert filter_rule.path == "event_message"
      assert filter_rule.value == "user authentication failed"
      assert filter_rule.modifiers.quoted_string == true
    end

    test "builds filter from regex pattern" do
      regex = ~s|~"error.*timeout"|

      {:ok, rules} = Lql.build_message_filter_from_regex(regex)

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
end
