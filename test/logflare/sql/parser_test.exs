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
    @terms 30_000

    defp or_chain(terms) do
      ["SELECT a FROM t WHERE a = 0" | Enum.map(1..(terms - 1), &" OR a = #{&1}")]
      |> IO.iodata_to_binary()
    end

    test "parse/2 refuses a deep OR chain before constructing its AST" do
      query = or_chain(@terms)
      assert byte_size(query) < Parser.max_query_bytes()

      for dialect <- ~w(bigquery clickhouse postgres) do
        assert {:error, message} = Parser.parse(dialect, query)
        assert message =~ "20000 significant tokens"
      end
    end

    test "parse_clickhouse_converted/1 refuses a deep OR chain" do
      assert {:error, message} = Parser.parse_clickhouse_converted(or_chain(@terms))
      assert message =~ "20000 significant tokens"
    end

    test "queries below the token budget are still checked for AST depth" do
      query = or_chain(100)

      for parse <- [
            &Parser.parse("bigquery", &1),
            &Parser.parse("clickhouse", &1),
            &Parser.parse("postgres", &1),
            &Parser.parse_clickhouse_converted/1
          ] do
        assert {:error, message} = parse.(query)
        assert message =~ "JSON recursion budget 128"
      end
    end

    test "to_string/1 returns an error for a hand-built AST too deep to deserialize" do
      assert {:ok, ast} = Parser.parse("clickhouse", "SELECT a FROM t WHERE a = 0")
      path = [Access.at(0), "Query", "body", "Select", "selection"]
      selection = Enum.reduce(1..200, get_in(ast, path), fn _, expr -> %{"Nested" => expr} end)
      assert {:error, message} = Parser.to_string(put_in(ast, path, selection))
      assert message =~ "recursion limit"
    end

    test "a broad shallow query at the token limit is accepted" do
      query = "SELECT " <> Enum.map_join(1..10_000, ",", fn _ -> "1" end)

      for parse <- [
            &Parser.parse("bigquery", &1),
            &Parser.parse("clickhouse", &1),
            &Parser.parse("postgres", &1),
            &Parser.parse_clickhouse_converted/1
          ] do
        assert {:ok, ast} = parse.(query)
        assert {:ok, _formatted} = Parser.to_string(ast)
        assert {:error, message} = parse.(query <> ";")
        assert message =~ "20000 significant tokens"
      end
    end

    test "comments and large literals with JSON punctuation do not consume the depth budget" do
      literal = String.duplicate(~s([{\\"\\\\}]), 3_000)
      query = "SELECT '" <> literal <> "' AS \"[{column}]\" /* " <> literal <> " */"

      for parse <- [
            &Parser.parse("bigquery", &1),
            &Parser.parse("clickhouse", &1),
            &Parser.parse("postgres", &1),
            &Parser.parse_clickhouse_converted/1
          ] do
        assert {:ok, ast} = parse.(query)
        assert {:ok, _formatted} = Parser.to_string(ast)
      end
    end

    test "large literals below and at the byte limit are accepted" do
      for bytes <- [Parser.max_query_bytes() - 1, Parser.max_query_bytes()],
          parse <- [
            &Parser.parse("bigquery", &1),
            &Parser.parse("clickhouse", &1),
            &Parser.parse("postgres", &1),
            &Parser.parse_clickhouse_converted/1
          ] do
        query = "SELECT '" <> String.duplicate("x", bytes - 9) <> "'"
        assert byte_size(query) == bytes
        assert {:ok, [_statement]} = parse.(query)
      end
    end

    test "queries over the size limit are refused before parsing" do
      query = "SELECT 1 " <> String.duplicate(" ", Parser.max_query_bytes())

      assert {:error, "Query is too large to parse" <> _} = Parser.parse("clickhouse", query)

      assert {:error, "Query is too large to parse" <> _} =
               Parser.parse_clickhouse_converted(query)
    end
  end

  describe "nested UNION round trips" do
    test "formats nested queries with 16 through 23 UNION ALLs without crashing" do
      columns = Enum.map_join(0..11, ", ", &"c#{&1}")
      first = "SELECT " <> Enum.map_join(0..11, ", ", &"'#{&1}' AS c#{&1}") <> " FROM t"

      for unions <- [16, 19, 20, 21, 22, 23],
          parse <- [&Parser.parse("clickhouse", &1), &Parser.parse_clickhouse_converted/1] do
        rows =
          Enum.map_join(1..unions, "", fn row ->
            " UNION ALL SELECT " <> Enum.map_join(0..11, ", ", &"'r#{row}_#{&1}'")
          end)

        query =
          Enum.reduce(1..5, first <> rows, fn _, inner ->
            "SELECT #{columns} FROM (#{inner})"
          end)

        assert {:ok, ast} = parse.(query)
        assert {:ok, formatted} = Parser.to_string(ast)
        assert {:ok, reparsed} = parse.(formatted)
        assert {:ok, ^formatted} = Parser.to_string(reparsed)
      end
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
