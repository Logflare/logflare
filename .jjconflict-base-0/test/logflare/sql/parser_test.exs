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

  describe "parse/2 with clickhouse SAMPLE clause" do
    test "Parses and round-trips a SAMPLE ratio" do
      query = "SELECT a FROM default.otel_logs SAMPLE 0.1"

      assert {:ok, ast} = Parser.parse("clickhouse", query)
      assert {:ok, ^query} = Parser.to_string(ast)
    end

    test "Parses and round-trips a SAMPLE fractional expression" do
      assert {:ok, ast} =
               Parser.parse("clickhouse", "SELECT a FROM default.otel_logs SAMPLE 1/10")

      assert {:ok, "SELECT a FROM default.otel_logs SAMPLE 1 / 10"} = Parser.to_string(ast)
    end

    test "Parses and round-trips a SAMPLE with OFFSET" do
      query = "SELECT a FROM default.otel_logs SAMPLE 0.1 OFFSET 0.5"

      assert {:ok, ast} = Parser.parse("clickhouse", query)
      assert {:ok, ^query} = Parser.to_string(ast)
    end
  end

  describe "parse/2 with clickhouse query-level SETTINGS" do
    test "Parses and round-trips a single setting" do
      query = "SELECT a FROM t SETTINGS max_threads = 4"

      assert {:ok, ast} = Parser.parse("clickhouse", query)
      assert {:ok, ^query} = Parser.to_string(ast)
    end

    test "Parses and round-trips multiple settings alongside GROUP BY" do
      query =
        "SELECT user_id, count() FROM events GROUP BY user_id SETTINGS max_threads = 4, max_execution_time = 10"

      assert {:ok, ast} = Parser.parse("clickhouse", query)
      assert {:ok, ^query} = Parser.to_string(ast)
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
