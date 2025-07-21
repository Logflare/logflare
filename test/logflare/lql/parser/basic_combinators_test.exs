defmodule Logflare.Lql.Parser.BasicCombinatorsTest do
  use ExUnit.Case, async: true

  import NimbleParsec

  alias Logflare.Lql.Parser.BasicCombinators

  defparsec(:test_null_parser, BasicCombinators.null())
  defparsec(:test_number_parser, BasicCombinators.number())
  defparsec(:test_any_field_parser, BasicCombinators.any_field())
  defparsec(:test_metadata_field_parser, BasicCombinators.metadata_field())
  defparsec(:test_operator_parser, BasicCombinators.operator())
  defparsec(:test_level_strings, BasicCombinators.level_strings())

  describe "basic parser combinators" do
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
  end

  describe "condition functions" do
    test "`not_quote` stops at unescaped quote" do
      assert BasicCombinators.not_quote("\"test", [], 0, 0) == {:halt, []}
    end

    test "`not_quote` continues for escaped quote" do
      assert BasicCombinators.not_quote("\\\"test", [], 0, 0) == {:cont, []}
    end

    test "`not_quote` continues for other characters" do
      assert BasicCombinators.not_quote("atest", [], 0, 0) == {:cont, []}
      assert BasicCombinators.not_quote("123test", [], 0, 0) == {:cont, []}
      assert BasicCombinators.not_quote(" test", [], 0, 0) == {:cont, []}
      assert BasicCombinators.not_quote("\\test", [], 0, 0) == {:cont, []}
    end

    test "`not_whitespace` stops at various whitespace characters" do
      assert BasicCombinators.not_whitespace(" test", [], 0, 0) == {:halt, []}
      assert BasicCombinators.not_whitespace("\ttest", [], 0, 0) == {:halt, []}
      assert BasicCombinators.not_whitespace("\ntest", [], 0, 0) == {:halt, []}
      assert BasicCombinators.not_whitespace("\vtest", [], 0, 0) == {:halt, []}
      assert BasicCombinators.not_whitespace("\rtest", [], 0, 0) == {:halt, []}
      assert BasicCombinators.not_whitespace("\ftest", [], 0, 0) == {:halt, []}
      assert BasicCombinators.not_whitespace("\btest", [], 0, 0) == {:halt, []}
      assert BasicCombinators.not_whitespace("", [], 0, 0) == {:halt, []}
    end

    test "`not_whitespace` continues for non-whitespace characters" do
      assert BasicCombinators.not_whitespace("atest", [], 0, 0) == {:cont, []}
      assert BasicCombinators.not_whitespace("1test", [], 0, 0) == {:cont, []}
      assert BasicCombinators.not_whitespace("_test", [], 0, 0) == {:cont, []}
      assert BasicCombinators.not_whitespace(".test", [], 0, 0) == {:cont, []}
    end

    test "`not_right_paren` stops at right parenthesis" do
      assert BasicCombinators.not_right_paren(")test", [], 0, 0) == {:halt, []}
    end

    test "`not_right_paren` continues for other characters" do
      assert BasicCombinators.not_right_paren("(test", [], 0, 0) == {:cont, []}
      assert BasicCombinators.not_right_paren("atest", [], 0, 0) == {:cont, []}
      assert BasicCombinators.not_right_paren("123test", [], 0, 0) == {:cont, []}
      assert BasicCombinators.not_right_paren(" test", [], 0, 0) == {:cont, []}
    end
  end
end
