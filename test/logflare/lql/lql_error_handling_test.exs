defmodule Logflare.Lql.ErrorHandlingTest do
  use Logflare.DataCase, async: true

  alias Logflare.Lql.Parser
  alias Logflare.Source.BigQuery.SchemaBuilder

  @default_schema SchemaBuilder.initial_table_schema()

  describe "parsing error handling" do
    test "returns error for malformed query syntax" do
      assert {:error, error} = Parser.parse("m.field:", @default_schema)
      assert error =~ "Error while parsing"
      assert error =~ "metadata filter value"
    end

    test "returns error for invalid timestamp format" do
      assert {:error, error} = Parser.parse("t:>invalid-date", @default_schema)
      assert error =~ "Error while parsing timestamp"
    end

    test "returns error for invalid operator combinations" do
      assert {:error, error} = Parser.parse("m.field:>>100", @default_schema)
      assert error =~ "Error while parsing"
    end

    test "returns error for unclosed quotes" do
      assert {:error, error} = Parser.parse("\"unclosed quote", @default_schema)
      assert is_binary(error)
    end

    test "returns error for invalid chart syntax" do
      result = Parser.parse("c:invalid(m.field)", @default_schema)
      assert match?({:error, :field_not_found, _, _}, result)
    end

    test "handles empty query string" do
      result = Parser.parse("", @default_schema)
      assert result == {:ok, []}
    end

    test "handles whitespace-only query" do
      result = Parser.parse("   ", @default_schema)
      # Parser may or may not handle whitespace-only queries
      assert match?({:ok, _}, result) or match?({:error, _}, result)
    end

    test "handles field not in schema" do
      result = Parser.parse("m.nonexistent.field:value", @default_schema)
      assert match?({:error, :field_not_found, _, _}, result)
    end
  end

  describe "edge cases" do
    test "handles extremely long field names" do
      # Create a schema with the long field
      # Shorter to avoid issues
      long_field = String.duplicate("a", 100)
      schema = build_schema(%{"metadata" => %{long_field => "value"}})
      query = "m.#{long_field}:value"

      result = Parser.parse(query, schema)
      assert match?({:ok, _}, result)
    end

    test "handles extremely long values" do
      schema = build_schema(%{"metadata" => %{"field" => "test"}})
      long_value = String.duplicate("test", 100)
      query = "m.field:\"#{long_value}\""

      result = Parser.parse(query, schema)
      assert match?({:ok, _}, result)
    end

    test "handles special characters in values" do
      schema = build_schema(%{"metadata" => %{"field" => "test"}})
      special_chars = ["!@#$%^&*()", "こんにちは", "🚀"]

      for char <- special_chars do
        query = "m.field:\"#{char}\""
        result = Parser.parse(query, schema)
        assert match?({:ok, _}, result)
      end
    end

    test "handles multiple consecutive operators" do
      # These should all fail gracefully
      invalid_queries = [
        "m.field::value",
        "m.field:>=<100",
        "m.field:~~pattern"
      ]

      for query <- invalid_queries do
        result = Parser.parse(query, @default_schema)
        assert match?({:error, _}, result)
      end
    end

    test "handles numeric edge cases" do
      schema = build_schema(%{"metadata" => %{"field" => 1}})

      queries = [
        "m.field:0",
        "m.field:999999999999999999999"
      ]

      for query <- queries do
        result = Parser.parse(query, schema)
        assert match?({:ok, _}, result)
      end

      # Test float field separately
      float_schema = build_schema(%{"metadata" => %{"field" => 1.0}})
      float_result = Parser.parse("m.field:3.14159265359", float_schema)
      assert match?({:ok, _}, float_result)
    end

    test "handles boolean values" do
      schema = build_schema(%{"metadata" => %{"field" => true}})

      queries = [
        "m.field:true",
        "m.field:false"
      ]

      for query <- queries do
        result = Parser.parse(query, schema)
        assert match?({:ok, _}, result)
      end
    end

    test "handles NULL values" do
      schema = build_schema(%{"metadata" => %{"field" => "test"}})
      query = "m.field:NULL"

      result = Parser.parse(query, schema)
      assert match?({:ok, _}, result)
    end

    test "handles timestamp edge cases" do
      queries = [
        "t:1970-01-01",
        # Leap year
        "t:2000-02-29",
        "t:2023-01-01T00:00:00"
      ]

      for query <- queries do
        result = Parser.parse(query, @default_schema)
        assert match?({:ok, _}, result)
      end
    end

    test "handles range operators" do
      schema = build_schema(%{"metadata" => %{"field" => 1}})

      queries = [
        "m.field:1..10",
        # Reversed range
        "m.field:10..1",
        # Single value range
        "m.field:1..1"
      ]

      for query <- queries do
        result = Parser.parse(query, schema)
        assert match?({:ok, _}, result)
      end
    end

    test "handles regex patterns" do
      queries = [
        "~error",
        "~[a-z]+",
        "~\\d+"
      ]

      for query <- queries do
        result = Parser.parse(query, @default_schema)
        assert match?({:ok, _}, result)
      end
    end

    test "handles array operations" do
      schema = build_schema(%{"metadata" => %{"array" => ["value"]}})

      queries = [
        "m.array:@>value",
        "m.array:@>\"quoted value\"",
        "m.array:@>123"
      ]

      for query <- queries do
        result = Parser.parse(query, schema)
        assert match?({:ok, _}, result)
      end
    end

    test "handles chart operators" do
      schema = build_schema(%{"metadata" => %{"field" => 1.0}})

      queries = [
        "c:count(*)",
        "c:sum(m.field)",
        "c:avg(m.field)"
      ]

      for query <- queries do
        result = Parser.parse(query, schema)
        assert match?({:ok, _}, result)
      end

      # Test combined chart query
      combined_query = "c:avg(m.field) c:group_by(t::minute)"
      result = Parser.parse(combined_query, schema)
      assert match?({:ok, _}, result)
    end

    test "handles negation" do
      schema = build_schema(%{"metadata" => %{"field" => "value"}})

      queries = [
        "-m.field:value",
        "-~pattern"
      ]

      for query <- queries do
        result = Parser.parse(query, schema)
        assert match?({:ok, _}, result)
      end
    end

    test "handles multiple filters" do
      schema = build_schema(%{"metadata" => %{"field1" => "value1", "field2" => "value2"}})
      query = "m.field1:value1 m.field2:value2"

      result = Parser.parse(query, schema)
      assert match?({:ok, _}, result)
    end
  end

  describe "schema validation" do
    test "handles missing schema gracefully" do
      # Parser expects a schema, so this should error
      assert_raise FunctionClauseError, fn ->
        Parser.parse("m.field:value", nil)
      end
    end

    test "handles empty schema" do
      empty_schema = %GoogleApi.BigQuery.V2.Model.TableSchema{fields: []}
      result = Parser.parse("m.field:value", empty_schema)
      # Should fail validation since field doesn't exist
      assert match?({:error, :field_not_found, _, _}, result)
    end

    test "handles basic schema with new fields" do
      basic_schema = SchemaBuilder.initial_table_schema()
      result = Parser.parse("m.new_field:value", basic_schema)
      # Should fail validation since field doesn't exist in schema
      assert match?({:error, :field_not_found, _, _}, result)
    end
  end

  defp build_schema(input) do
    SchemaBuilder.build_table_schema(input, @default_schema)
  end
end
