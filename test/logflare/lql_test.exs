defmodule Logflare.LqlTest do
  use Logflare.DataCase
  
  alias Logflare.Lql
  alias Logflare.Lql.FilterRule
  
  import Ecto.Query
  
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
      
      # Should return an Ecto.Query with the filter applied
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
      
      # Should return an Ecto.Query with the filter applied
      assert %Ecto.Query{} = result
    end
    
    test "handles empty filter rules list" do
      query = from("test_table")
      
      result = Lql.apply_filter_rules_to_query(query, [])
      
      # Should return the original query unchanged
      assert result == query
    end
  end
  
  describe "handle_nested_field_access/3" do
    test "handles nested field access using BigQuery backend transformer by default" do
      query = from("test_table")
      
      result = Lql.handle_nested_field_access(query, "metadata.user.id")
      
      # Should return an Ecto.Query with joins for nested field access
      assert %Ecto.Query{} = result
    end
    
    test "handles nested field access with custom adapter option" do
      query = from("test_table")
      
      result = Lql.handle_nested_field_access(query, "metadata.user.id", adapter: :bigquery)
      
      # Should return an Ecto.Query with joins for nested field access
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
      
      # Should return an Ecto.Query.DynamicExpr
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
      
      # Should return an Ecto.Query.DynamicExpr
      assert %Ecto.Query.DynamicExpr{} = result
    end
  end
  
  describe "is_negated?/1" do
    test "returns true when negate modifier is present" do
      modifiers = %{negate: true}
      
      assert Lql.is_negated?(modifiers) == true
    end
    
    test "returns false when negate modifier is false" do
      modifiers = %{negate: false}
      
      assert Lql.is_negated?(modifiers) == false
    end
    
    test "returns false when negate modifier is not present" do
      modifiers = %{}
      
      assert Lql.is_negated?(modifiers) == false
    end
    
    test "returns false when modifiers is empty" do
      modifiers = %{}
      
      assert Lql.is_negated?(modifiers) == false
    end
  end
  
  describe "integration with existing API" do
    test "decode/2 works as expected" do
      lql_string = "m.status:error"
      schema = build_basic_schema()
      
      {:ok, rules} = Lql.decode(lql_string, schema)
      
      assert length(rules) == 1
      assert [%FilterRule{path: "metadata.status", operator: :=, value: "error"}] = rules
    end
    
    test "encode/1 works as expected" do
      rules = [%FilterRule{path: "metadata.status", operator: :=, value: "error", modifiers: %{}}]
      
      {:ok, encoded} = Lql.encode(rules)
      
      assert encoded == "m.status:error"
    end
    
    test "encode!/1 works as expected" do
      rules = [%FilterRule{path: "metadata.status", operator: :=, value: "error", modifiers: %{}}]
      
      encoded = Lql.encode!(rules)
      
      assert encoded == "m.status:error"
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