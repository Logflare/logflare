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
    test "builds SelectRule struct from specific field path" do
      result = SelectRule.build_from_path("metadata.user.id")

      assert %SelectRule{} = result
      assert result.path == "metadata.user.id"
      assert result.wildcard == false
    end

    test "builds SelectRule struct from wildcard path" do
      result = SelectRule.build_from_path("*")

      assert %SelectRule{} = result
      assert result.path == "*"
      assert result.wildcard == true
    end

    test "builds SelectRule struct from nil path defaults to wildcard" do
      result = SelectRule.build_from_path(nil)

      assert %SelectRule{} = result
      assert result.path == "*"
      assert result.wildcard == true
    end

    test "builds SelectRule struct from any field path" do
      result = SelectRule.build_from_path("event_message")

      assert %SelectRule{} = result
      assert result.path == "event_message"
      assert result.wildcard == false
    end
  end

  describe "build/1" do
    test "builds select rule from keyword list" do
      params = [path: "metadata.user_id", wildcard: false]
      result = SelectRule.build(params)

      assert %SelectRule{} = result
      assert result.path == "metadata.user_id"
      assert result.wildcard == false
    end

    test "builds wildcard select rule from keyword list" do
      params = [path: "*", wildcard: true]
      result = SelectRule.build(params)

      assert %SelectRule{} = result
      assert result.path == "*"
      assert result.wildcard == true
    end

    test "builds with partial params" do
      params = [path: "event_message"]
      result = SelectRule.build(params)

      assert %SelectRule{} = result
      assert result.path == "event_message"
      assert result.wildcard == false
    end

    test "returns empty struct for invalid changeset" do
      params = [path: "invalid-field-name!"]
      result = SelectRule.build(params)

      assert %SelectRule{} = result
      assert result.path == nil
      assert result.wildcard == false
    end
  end

  describe "changeset/2" do
    test "creates valid changeset with valid data" do
      rule = %SelectRule{}
      params = %{path: "metadata.user_id"}

      changeset = SelectRule.changeset(rule, params)

      assert changeset.valid?
      assert changeset.changes.path == "metadata.user_id"
      assert changeset.changes.wildcard == false
    end

    test "creates valid changeset for wildcard" do
      rule = %SelectRule{}
      params = %{path: "*"}

      changeset = SelectRule.changeset(rule, params)

      assert changeset.valid?
      assert changeset.changes.path == "*"
      assert changeset.changes.wildcard == true
    end

    test "creates invalid changeset for invalid path" do
      rule = %SelectRule{}
      params = %{path: "invalid-field-name!"}

      changeset = SelectRule.changeset(rule, params)

      refute changeset.valid?
      assert changeset.errors[:path]
    end

    test "creates invalid changeset for missing path" do
      rule = %SelectRule{}
      params = %{}

      changeset = SelectRule.changeset(rule, params)

      refute changeset.valid?
      assert changeset.errors[:path]
    end

    test "validates deeply nested field paths" do
      rule = %SelectRule{}
      params = %{path: "user.profile.settings.theme.colors.primary.dark_mode"}

      changeset = SelectRule.changeset(rule, params)

      assert changeset.valid?
      assert changeset.changes.path == "user.profile.settings.theme.colors.primary.dark_mode"
      assert changeset.changes.wildcard == false
    end

    test "creates changeset from existing struct" do
      existing_rule = %SelectRule{path: "metadata.user_id", wildcard: false}

      changeset = SelectRule.changeset(nil, existing_rule)

      assert changeset.valid?
      assert changeset.changes == %{}
      assert changeset.data == existing_rule
    end
  end

  describe "virtual_fields/0" do
    test "returns list of virtual field names" do
      virtual_fields = SelectRule.virtual_fields()

      assert is_list(virtual_fields)
      assert :path in virtual_fields
      assert :wildcard in virtual_fields
    end
  end

  describe "normalize/1" do
    test "applies wildcard precedence when wildcard present" do
      rules = [
        %SelectRule{path: "field1", wildcard: false},
        %SelectRule{path: "*", wildcard: true},
        %SelectRule{path: "field2", wildcard: false}
      ]

      normalized = SelectRule.normalize(rules)

      assert length(normalized) == 1
      assert hd(normalized).path == "*"
      assert hd(normalized).wildcard == true
    end

    test "deduplicates fields when no wildcard" do
      rules = [
        %SelectRule{path: "field1", wildcard: false},
        %SelectRule{path: "field2", wildcard: false},
        %SelectRule{path: "field1", wildcard: false}
      ]

      normalized = SelectRule.normalize(rules)

      assert length(normalized) == 2
      paths = Enum.map(normalized, & &1.path)
      assert "field1" in paths
      assert "field2" in paths
    end

    test "handles empty list" do
      normalized = SelectRule.normalize([])
      assert normalized == []
    end

    test "handles single wildcard rule" do
      rules = [%SelectRule{path: "*", wildcard: true}]
      normalized = SelectRule.normalize(rules)

      assert length(normalized) == 1
      assert hd(normalized).wildcard == true
    end
  end

  describe "apply_wildcard_precedence/1" do
    test "returns first wildcard rule when wildcards present" do
      rules = [
        %SelectRule{path: "field1", wildcard: false},
        %SelectRule{path: "*", wildcard: true},
        %SelectRule{path: "field2", wildcard: false},
        # another wildcard
        %SelectRule{path: "**", wildcard: true}
      ]

      result = SelectRule.apply_wildcard_precedence(rules)

      assert length(result) == 1
      assert hd(result).path == "*"
    end

    test "returns all rules when no wildcards" do
      rules = [
        %SelectRule{path: "field1", wildcard: false},
        %SelectRule{path: "field2", wildcard: false}
      ]

      result = SelectRule.apply_wildcard_precedence(rules)

      assert length(result) == 2
      assert result == rules
    end
  end

  describe "deduplicate_paths/1" do
    test "removes duplicate paths" do
      rules = [
        %SelectRule{path: "field1", wildcard: false},
        %SelectRule{path: "field2", wildcard: false},
        %SelectRule{path: "field1", wildcard: false},
        %SelectRule{path: "field3", wildcard: false},
        %SelectRule{path: "field2", wildcard: false}
      ]

      result = SelectRule.deduplicate_paths(rules)

      assert length(result) == 3
      paths = Enum.map(result, & &1.path)
      assert "field1" in paths
      assert "field2" in paths
      assert "field3" in paths
    end

    test "preserves first occurrence of duplicate paths" do
      original_rule = %SelectRule{path: "field1", wildcard: false}

      rules = [
        original_rule,
        %SelectRule{path: "field1", wildcard: false}
      ]

      result = SelectRule.deduplicate_paths(rules)

      assert length(result) == 1
      assert hd(result) == original_rule
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
