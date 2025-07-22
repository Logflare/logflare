defmodule Logflare.Lql.Rules.SelectRuleTest do
  use ExUnit.Case, async: true

  alias Logflare.Lql.Rules.SelectRule

  describe "__struct__" do
    test "creates struct with default values" do
      select_rule = %SelectRule{}

      assert select_rule.path == nil
      assert select_rule.wildcard == false
    end

    test "creates struct with custom values" do
      select_rule = %SelectRule{
        path: "metadata.user_id",
        wildcard: false
      }

      assert select_rule.path == "metadata.user_id"
      assert select_rule.wildcard == false
    end

    test "creates struct for wildcard selection" do
      select_rule = %SelectRule{
        path: "*",
        wildcard: true
      }

      assert select_rule.path == "*"
      assert select_rule.wildcard == true
    end
  end

  describe "build_from_path/1" do
    test "builds result from specific field path" do
      result = SelectRule.build_from_path("metadata.user.id")

      assert is_map(result)
      assert result.path == "metadata.user.id"
      assert result.wildcard == false
    end

    test "builds result from wildcard path" do
      result = SelectRule.build_from_path("*")

      assert is_map(result)
      assert result.path == "*"
      assert result.wildcard == true
    end

    test "builds result from nil path defaults to wildcard" do
      result = SelectRule.build_from_path(nil)

      assert is_map(result)
      assert result.path == "*"
      assert result.wildcard == true
    end

    test "builds result from any field path" do
      result = SelectRule.build_from_path("event_message")

      assert is_map(result)
      assert result.path == "event_message"
      assert result.wildcard == false
    end
  end

  describe "Jason.Encoder" do
    test "encodes select rule to JSON" do
      select_rule = %SelectRule{
        path: "metadata.user_id",
        wildcard: false
      }

      json = Jason.encode!(select_rule)
      decoded = Jason.decode!(json)

      assert decoded["path"] == "metadata.user_id"
      assert decoded["wildcard"] == false
    end

    test "encodes wildcard select rule" do
      select_rule = %SelectRule{
        path: "*",
        wildcard: true
      }

      json = Jason.encode!(select_rule)
      decoded = Jason.decode!(json)

      assert decoded["path"] == "*"
      assert decoded["wildcard"] == true
    end
  end
end
