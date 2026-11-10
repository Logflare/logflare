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

  describe "deep ASTs do not crash the VM" do
    # A long OR chain parses into an AST one level deeper per term. Before the parser NIF ran on
    # its own large stack, 20,000 terms overflowed the BEAM scheduler stack while the NIF
    # serialized the AST, and the VM died with a segfault. These tests then killed the test run.
    @terms 30_000

    defp or_chain(terms) do
      ["SELECT a FROM t WHERE a = 0" | Enum.map(1..(terms - 1), &" OR a = #{&1}")]
      |> IO.iodata_to_binary()
    end

    test "parse/2 handles a deep OR chain" do
      query = or_chain(@terms)
      assert byte_size(query) < Parser.max_query_bytes()

      for dialect <- ~w(bigquery clickhouse postgres) do
        assert {:ok, [_statement]} = Parser.parse(dialect, query)
      end
    end

    test "parse_clickhouse_converted/1 handles a deep OR chain" do
      assert {:ok, [_statement]} = Parser.parse_clickhouse_converted(or_chain(@terms))
    end

    test "to_string/1 returns an error for an AST too deep to deserialize" do
      assert {:ok, ast} = Parser.parse("clickhouse", or_chain(@terms))
      assert {:error, message} = Parser.to_string(ast)
      assert message =~ "recursion limit"
    end

    test "queries over the size limit are refused before parsing" do
      query = "SELECT 1 " <> String.duplicate(" ", Parser.max_query_bytes())

      assert {:error, "Query is too large to parse" <> _} = Parser.parse("clickhouse", query)

      assert {:error, "Query is too large to parse" <> _} =
               Parser.parse_clickhouse_converted(query)
    end
  end

  describe "internal converted ClickHouse SQL" do
    test "customer SQL does not accept converter-only positional bindings" do
      for sql <- ["SELECT {$0:String}", "SELECT a FROM {$0:Identifier}"] do
        assert {:error, _} = Parser.parse("clickhouse", sql)
      end
    end

    test "round-trips native types, repeated names and parameter-shaped literals" do
      sql =
        "SELECT {$0:String}, {$1:Array(UInt64)}, {$0:String}, '{$0:String}' AS literal " <>
          "FROM t WHERE ts = {$2:DateTime64(6, 'UTC')}"

      assert {:ok, ast} = Parser.parse_clickhouse_converted(sql)
      assert {:ok, formatted} = Parser.to_string(ast)
      assert formatted =~ "{$0: String}"
      assert formatted =~ "{$1: Array(UInt64)}"
      assert formatted =~ "{$2: DateTime64(6, 'UTC')}"
      assert formatted =~ "'{$0:String}' AS literal"
      assert {:ok, reparsed} = Parser.parse_clickhouse_converted(formatted)
      assert {:ok, ^formatted} = Parser.to_string(reparsed)
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

  describe "parse/2 with clickhouse FINAL and ARRAY JOIN" do
    test "round-trips FINAL on an unaliased table" do
      for query <- [
            "SELECT a FROM t FINAL",
            "SELECT a FROM db.t FINAL WHERE b = 1",
            "SELECT count() FROM t FINAL GROUP BY a"
          ] do
        assert {:ok, ast} = Parser.parse("clickhouse", query)
        assert {:ok, ^query} = Parser.to_string(ast)
      end
    end

    test "round-trips ARRAY JOIN" do
      for query <- [
            "SELECT a, arr FROM t ARRAY JOIN arr",
            "SELECT a, x FROM t LEFT ARRAY JOIN arr AS x WHERE x > 1",
            "SELECT a FROM t FINAL ARRAY JOIN arr AS x, other AS y"
          ] do
        assert {:ok, ast} = Parser.parse("clickhouse", query)
        assert {:ok, ^query} = Parser.to_string(ast)
      end
    end

    test "rejects FINAL after an explicit alias" do
      assert {:error, _reason} = Parser.parse("clickhouse", "SELECT a FROM t AS x FINAL")
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

    test "returns an error instead of raising when the AST is malformed" do
      assert {:error, message} = Parser.to_string(%{"Query" => %{}})
      assert message =~ "missing field"
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
    |> Enum.map(&Map.fetch!(&1, "Identifier"))
  end
end
