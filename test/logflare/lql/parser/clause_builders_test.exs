defmodule Logflare.Lql.Parser.ClauseBuildersTest do
  use ExUnit.Case, async: true
  use LqlParserTestHelpers

  alias Logflare.Lql.Parser.ClauseBuilders

  defparsec(:test_timestamp_clause, ClauseBuilders.timestamp_clause())
  defparsec(:test_metadata_clause, ClauseBuilders.metadata_clause())
  defparsec(:test_field_clause, ClauseBuilders.field_clause())
  defparsec(:test_metadata_level_clause, ClauseBuilders.metadata_level_clause())

  describe "clause builders" do
    test "timestamp clause parser can parse basic timestamp queries" do
      result = test_timestamp_clause("timestamp:now")
      assert match?({:ok, [%{path: "timestamp"} | _], _, _, _, _}, result)

      result = test_timestamp_clause("t:today")
      assert match?({:ok, [%{path: "timestamp"} | _], _, _, _, _}, result)
    end

    test "metadata clause parser can parse metadata fields" do
      result = test_metadata_clause("metadata.level:info")
      assert match?({:ok, [%{path: "metadata.level"} | _], _, _, _, _}, result)

      result = test_metadata_clause("m.duration:123")
      assert match?({:ok, [%{path: "metadata.duration"} | _], _, _, _, _}, result)
    end

    test "field clause parser can parse custom fields" do
      result = test_field_clause("custom_field:test")
      assert match?({:ok, [%{path: "custom_field"} | _], _, _, _, _}, result)

      result = test_field_clause("field.name:NULL")
      assert match?({:ok, [%{path: "field.name"} | _], _, _, _, _}, result)
    end

    test "metadata level clause parser can parse level ranges" do
      result = test_metadata_level_clause("metadata.level:info..error")
      assert match?({:ok, [[%{path: "metadata.level"} | _]], _, _, _, _}, result)
    end
  end
end
