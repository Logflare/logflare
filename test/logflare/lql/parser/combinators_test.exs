defmodule Logflare.Lql.Parser.CombinatorsTest do
  use ExUnit.Case, async: true

  import NimbleParsec
  import Logflare.Lql.Parser.Combinators
  import Logflare.Lql.Parser.Helpers

  alias Logflare.Lql.Parser.Combinators

  # Basic combinator parsers
  defparsec(:test_null_parser, Combinators.null())
  defparsec(:test_number_parser, Combinators.number())
  defparsec(:test_any_field_parser, Combinators.any_field())
  defparsec(:test_metadata_field_parser, Combinators.metadata_field())
  defparsec(:test_operator_parser, Combinators.operator())
  defparsec(:test_level_strings, Combinators.level_strings())
  defparsec(:test_word_parser, Combinators.word())

  # Chart combinator parsers
  defparsec(:test_chart_clause, Combinators.chart_clause())
  defparsec(:test_chart_aggregate, Combinators.chart_aggregate())
  defparsec(:test_chart_aggregate_group_by, Combinators.chart_aggregate_group_by())

  # DateTime combinator parsers
  defparsec(:test_datetime_abbreviations, Combinators.datetime_abbreviations())
  defparsec(:test_date, Combinators.date())
  defparsec(:test_datetime, Combinators.datetime())
  defparsec(:test_timestamp_shorthand_value, Combinators.timestamp_shorthand_value())
  defparsec(:test_timestamp_value, Combinators.timestamp_value())

  # Clause combinator parsers
  defparsec(:test_timestamp_clause, Combinators.timestamp_clause())
  defparsec(:test_metadata_clause, Combinators.metadata_clause())
  defparsec(:test_field_clause, Combinators.field_clause())
  defparsec(:test_select_clause, Combinators.select_clause())
  defparsec(:test_metadata_level_clause, Combinators.metadata_level_clause())

  describe "basic combinators" do
    test "null parser" do
      assert {:ok, [:NULL], "", _, _, _} = test_null_parser("NULL")
    end

    test "number parser" do
      assert {:ok, ["123"], "", _, _, _} = test_number_parser("123")
      assert {:ok, ["123.456"], "", _, _, _} = test_number_parser("123.456")
    end

    test "any field parser" do
      assert {:ok, [path: "field_name"], "", _, _, _} = test_any_field_parser("field_name")
      assert {:ok, [path: "field.name"], "", _, _, _} = test_any_field_parser("field.name")
    end

    test "metadata field parser" do
      assert {:ok, [path: "metadata.level"], "", _, _, _} =
               test_metadata_field_parser("metadata.level")

      assert {:ok, [path: "metadata.level"], "", _, _, _} = test_metadata_field_parser("m.level")
    end

    test "operator parser" do
      assert {:ok, [operator: :=], "", _, _, _} = test_operator_parser(":")
      assert {:ok, [operator: :>=], "", _, _, _} = test_operator_parser(":>=")
      assert {:ok, [operator: :"~"], "", _, _, _} = test_operator_parser(":~")
      assert {:ok, [operator: :list_includes], "", _, _, _} = test_operator_parser(":@>")
      assert {:ok, [operator: :list_includes_regexp], "", _, _, _} = test_operator_parser(":@>~")
    end

    test "level strings parser" do
      assert {:ok, [0], "", _, _, _} = test_level_strings("debug")
      assert {:ok, [1], "", _, _, _} = test_level_strings("info")
      assert {:ok, [2], "", _, _, _} = test_level_strings("notice")
      assert {:ok, [3], "", _, _, _} = test_level_strings("warning")
      assert {:ok, [4], "", _, _, _} = test_level_strings("error")
      assert {:ok, [5], "", _, _, _} = test_level_strings("critical")
      assert {:ok, [6], "", _, _, _} = test_level_strings("alert")
      assert {:ok, [7], "", _, _, _} = test_level_strings("emergency")
    end

    test "word parser creates `FilterRule` for event_message" do
      result = test_word_parser("error")
      assert match?({:ok, [%{path: "event_message", value: "error"}], "", _, _, _}, result)

      result = test_word_parser("~pattern")

      assert match?(
               {:ok, [%{path: "event_message", operator: :"~", value: "pattern"}], "", _, _, _},
               result
             )
    end
  end

  describe "chart combinators" do
    test "chart clause parser" do
      assert {:ok, [chart: [aggregate: :count, path: "timestamp"]], "", _, _, _} =
               test_chart_clause("chart:count(*)")

      assert {:ok, [chart: [aggregate: :sum, path: "metadata.level"]], "", _, _, _} =
               test_chart_clause("c:sum(metadata.level)")
    end

    test "chart aggregate parser" do
      assert {:ok, [aggregate: :count, path: "timestamp"], "", _, _, _} =
               test_chart_aggregate("count(*)")

      assert {:ok, [aggregate: :avg, path: "metadata.duration"], "", _, _, _} =
               test_chart_aggregate("avg(metadata.duration)")
    end

    test "chart aggregate group by parser" do
      assert {:ok, [period: :minute], "", _, _, _} =
               test_chart_aggregate_group_by("group_by(t::m)")

      assert {:ok, [period: :hour], "", _, _, _} =
               test_chart_aggregate_group_by("group_by(timestamp::hour)")
    end
  end

  describe "datetime combinators" do
    test "datetime abbreviations parser" do
      assert {:ok, [:days], "", _, _, _} = test_datetime_abbreviations("days")
      assert {:ok, [:hours], "", _, _, _} = test_datetime_abbreviations("hours")
      assert {:ok, [:minutes], "", _, _, _} = test_datetime_abbreviations("m")
      assert {:ok, [:weeks], "", _, _, _} = test_datetime_abbreviations("w")
    end

    test "date parser" do
      result = test_date("2023-01-15")
      assert match?({:ok, [~D[2023-01-15]], "", _, _, _}, result)
    end

    test "datetime parser" do
      result = test_datetime("2023-01-15T10:30:00Z")
      assert match?({:ok, [~N[2023-01-15 10:30:00]], "", _, _, _}, result)
    end

    test "timestamp shorthand value parser" do
      result = test_timestamp_shorthand_value("now")
      assert match?({:ok, [%{shorthand: "now"}], "", _, _, _}, result)

      result = test_timestamp_shorthand_value("today")
      assert match?({:ok, [%{shorthand: "today"}], "", _, _, _}, result)

      result = test_timestamp_shorthand_value("this@hour")
      assert match?({:ok, [%{shorthand: "this@hours"}], "", _, _, _}, result)
    end

    test "timestamp value parser" do
      result = test_timestamp_value("2023-01-15")
      # The timestamp value parser can return different formats depending on which parser matches
      assert match?({:ok, [value: ~D[2023-01-15]], "", _, _, _}, result) or
               match?(
                 {:ok, [value: {:datetime_with_range, [[~D[2023-01-15]]]}], "", _, _, _},
                 result
               )

      result = test_timestamp_value("now")
      assert match?({:ok, [value: %{shorthand: "now"}], "", _, _, _}, result)
    end
  end

  describe "clause combinators" do
    test "timestamp clause parser" do
      result = test_timestamp_clause("timestamp:2023-01-15")
      assert match?({:ok, [%{path: "timestamp"}], "", _, _, _}, result)

      result = test_timestamp_clause("t:>2023-01-15T10:00:00Z")
      assert match?({:ok, [%{path: "timestamp", operator: :>}], "", _, _, _}, result)
    end

    test "metadata clause parser" do
      result = test_metadata_clause("metadata.level:error")
      assert match?({:ok, [%{path: "metadata.level", value: "error"}], "", _, _, _}, result)

      result = test_metadata_clause("m.user_id:123")
      assert match?({:ok, [%{path: "metadata.user_id", value: "123"}], "", _, _, _}, result)
    end

    test "field clause parser" do
      result = test_field_clause("status:200")
      assert match?({:ok, [%{path: "status", value: "200"}], "", _, _, _}, result)

      result = test_field_clause("user.name:~john")
      assert match?({:ok, [%{path: "user.name", operator: :"~"}], "", _, _, _}, result)
    end
  end

  describe "condition functions" do
    test "`not_quote` stops at unescaped quote" do
      assert Combinators.not_quote("\"test", [], 0, 0) == {:halt, []}
    end

    test "`not_quote` continues for escaped quote" do
      assert Combinators.not_quote("\\\"test", [], 0, 0) == {:cont, []}
    end

    test "`not_quote` continues for other characters" do
      assert Combinators.not_quote("atest", [], 0, 0) == {:cont, []}
      assert Combinators.not_quote("123test", [], 0, 0) == {:cont, []}
      assert Combinators.not_quote(" test", [], 0, 0) == {:cont, []}
      assert Combinators.not_quote("\\test", [], 0, 0) == {:cont, []}
    end

    test "`not_whitespace` stops at various whitespace characters" do
      assert Combinators.not_whitespace(" test", [], 0, 0) == {:halt, []}
      assert Combinators.not_whitespace("\ttest", [], 0, 0) == {:halt, []}
      assert Combinators.not_whitespace("\ntest", [], 0, 0) == {:halt, []}
      assert Combinators.not_whitespace("\vtest", [], 0, 0) == {:halt, []}
      assert Combinators.not_whitespace("\rtest", [], 0, 0) == {:halt, []}
      assert Combinators.not_whitespace("\ftest", [], 0, 0) == {:halt, []}
      assert Combinators.not_whitespace("\btest", [], 0, 0) == {:halt, []}
      assert Combinators.not_whitespace("", [], 0, 0) == {:halt, []}
    end

    test "`not_whitespace` continues for non-whitespace characters" do
      assert Combinators.not_whitespace("atest", [], 0, 0) == {:cont, []}
      assert Combinators.not_whitespace("1test", [], 0, 0) == {:cont, []}
      assert Combinators.not_whitespace("_test", [], 0, 0) == {:cont, []}
      assert Combinators.not_whitespace(".test", [], 0, 0) == {:cont, []}
    end

    test "`not_right_paren` stops at right parenthesis" do
      assert Combinators.not_right_paren(")test", [], 0, 0) == {:halt, []}
    end

    test "`not_right_paren` continues for other characters" do
      assert Combinators.not_right_paren("(test", [], 0, 0) == {:cont, []}
      assert Combinators.not_right_paren("atest", [], 0, 0) == {:cont, []}
      assert Combinators.not_right_paren("123test", [], 0, 0) == {:cont, []}
      assert Combinators.not_right_paren(" test", [], 0, 0) == {:cont, []}
    end
  end

  describe "select_clause" do
    test "parses wildcard select clause" do
      assert {:ok, [{:select, [path: "*"]}], "", _, _, _} = test_select_clause("s:*")
    end

    test "parses field select clause" do
      assert {:ok, [{:select, [path: "event_message"]}], "", _, _, _} =
               test_select_clause("s:event_message")
    end

    test "parses metadata select clause" do
      assert {:ok, [{:select, [path: "metadata.user.id"]}], "", _, _, _} =
               test_select_clause("s:m.user.id")
    end

    test "parses select clause with full syntax" do
      assert {:ok, [{:select, [path: "timestamp"]}], "", _, _, _} =
               test_select_clause("select:timestamp")
    end

    test "parses deeply nested field select clause" do
      assert {:ok, [{:select, [path: "request.headers.authorization.bearer.token"]}], "", _, _, _} =
               test_select_clause("s:request.headers.authorization.bearer.token")
    end
  end

  describe "metadata_level_clause" do
    test "parses single level range" do
      assert {:ok, [rules], "", _, _, _} =
               test_metadata_level_clause("metadata.level:error..error")

      assert is_list(rules)
      assert length(rules) == 1

      [rule] = rules
      assert %Logflare.Lql.Rules.FilterRule{} = rule
      assert rule.path == "metadata.level"
      assert rule.operator == :=
      assert rule.value == "error"
    end

    test "parses level range" do
      assert {:ok, [rules], "", _, _, _} =
               test_metadata_level_clause("metadata.level:debug..error")

      assert is_list(rules)
      assert length(rules) == 5

      paths = Enum.map(rules, & &1.path)
      operators = Enum.map(rules, & &1.operator)
      values = Enum.map(rules, & &1.value)

      assert Enum.all?(paths, &(&1 == "metadata.level"))
      assert Enum.all?(operators, &(&1 == :=))
      assert "debug" in values
      assert "error" in values
    end
  end
end
