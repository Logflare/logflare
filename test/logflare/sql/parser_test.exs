defmodule Logflare.Sql.ParserTest do
  use ExUnit.Case, async: true

  alias Logflare.Sql.Parser
  doctest Parser

  describe "parse/2" do
    test "Parses a simple bigquery SQL statement" do
      assert {:ok, parsed_query} =
               Parser.parse("bigquery", "SELECT * FROM `foo.bar.baz`")

      table_name_parts = extract_table_name_parts_from_ast(parsed_query)

      assert is_list(table_name_parts)
      assert Enum.count(table_name_parts) == 3
      assert Enum.all?(table_name_parts, &(Map.get(&1, "quote_style") == "`"))
      assert Enum.all?(table_name_parts, &(Map.get(&1, "value") in ~w(foo bar baz)))
    end

    test "Parses a simple clickhouse SQL statement" do
      assert {:ok, parsed_query} =
               Parser.parse("clickhouse", "SELECT * FROM default.foo")

      table_name_parts = extract_table_name_parts_from_ast(parsed_query)

      assert is_list(table_name_parts)
      assert Enum.count(table_name_parts) == 2
      assert Enum.all?(table_name_parts, &(Map.get(&1, "quote_style") == nil))
      assert Enum.all?(table_name_parts, &(Map.get(&1, "value") in ~w(default foo)))
    end

    test "Parses a simple postgres SQL statement" do
      assert {:ok, parsed_query} =
               Parser.parse("postgres", "SELECT * FROM foo;")

      table_name_parts = extract_table_name_parts_from_ast(parsed_query)

      assert is_list(table_name_parts)
      assert Enum.count(table_name_parts) == 1
      assert Enum.all?(table_name_parts, &(Map.get(&1, "quote_style") == nil))
      assert Enum.all?(table_name_parts, &(Map.get(&1, "value") == "foo"))
    end
  end

  describe "to_string/1" do
    test "Converts a parsed query AST back to a string" do
      bq_query = "SELECT * FROM `foo`.`bar`.`baz`"

      assert {:ok, parsed_query} = Parser.parse("bigquery", bq_query)
      assert {:ok, bq_query} == Parser.to_string(parsed_query)
    end
  end

  defp extract_table_name_parts_from_ast([%{} = ast]) do
    ast
    |> Map.get("Query")
    |> Map.get("body")
    |> Map.get("Select")
    |> Map.get("from")
    |> List.first()
    |> Map.get("relation")
    |> Map.get("Table")
    |> Map.get("name")
  end
end
