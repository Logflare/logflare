defmodule Logflare.Lql.Rules.FilterRuleTest do
  use ExUnit.Case, async: true

  alias Logflare.Lql.Rules.FilterRule

  describe "__struct__" do
    test "creates struct with default values" do
      filter_rule = %FilterRule{}

      assert filter_rule.path == nil
      assert filter_rule.operator == nil
      assert filter_rule.value == nil
      assert filter_rule.values == nil
      assert filter_rule.modifiers == %{}
      assert filter_rule.shorthand == nil
    end

    test "creates struct with custom values" do
      filter_rule = %FilterRule{
        path: "metadata.user_id",
        operator: :=,
        value: "123",
        modifiers: %{quoted_string: true},
        shorthand: nil
      }

      assert filter_rule.path == "metadata.user_id"
      assert filter_rule.operator == :=
      assert filter_rule.value == "123"
      assert filter_rule.modifiers == %{quoted_string: true}
      assert filter_rule.shorthand == nil
    end

    test "creates struct with range values" do
      filter_rule = %FilterRule{
        path: "metadata.count",
        operator: :range,
        values: [1, 100]
      }

      assert filter_rule.path == "metadata.count"
      assert filter_rule.operator == :range
      assert filter_rule.values == [1, 100]
      assert filter_rule.value == nil
    end
  end

  describe "changeset/2" do
    test "creates changeset from existing FilterRule struct" do
      filter_rule = %FilterRule{
        path: "event_message",
        operator: :=,
        value: "test"
      }

      changeset = FilterRule.changeset(%FilterRule{}, filter_rule)

      assert %Ecto.Changeset{} = changeset
      assert changeset.valid?
    end

    test "creates changeset from params map" do
      params = %{
        path: "metadata.level",
        operator: :=,
        value: "info"
      }

      changeset = FilterRule.changeset(%FilterRule{}, params)

      assert %Ecto.Changeset{} = changeset
      assert changeset.valid?
    end

    test "creates changeset with string keys" do
      params = %{
        "path" => "event_message",
        "operator" => :string_contains,
        "value" => "error"
      }

      changeset = FilterRule.changeset(%FilterRule{}, params)

      assert %Ecto.Changeset{} = changeset
      assert changeset.valid?
    end

    test "creates changeset with modifiers" do
      params = %{
        path: "event_message",
        operator: :string_contains,
        value: "test message",
        modifiers: %{quoted_string: true, negate: false}
      }

      changeset = FilterRule.changeset(%FilterRule{}, params)

      assert %Ecto.Changeset{} = changeset
      assert changeset.valid?
    end

    test "creates changeset with range values" do
      params = %{
        path: "timestamp",
        operator: :range,
        values: [~N[2020-01-01 00:00:00], ~N[2020-01-02 00:00:00]]
      }

      changeset = FilterRule.changeset(%FilterRule{}, params)

      assert %Ecto.Changeset{} = changeset
      assert changeset.valid?
    end
  end

  describe "build/1" do
    test "builds result from keyword list" do
      params = [
        path: "metadata.user",
        operator: :=,
        value: "alice"
      ]

      result = FilterRule.build(params)

      assert %FilterRule{} = result
      assert result.path == "metadata.user"
      assert result.operator == :=
      assert result.value == "alice"
    end

    test "builds result with modifiers from keyword list" do
      params = [
        path: "event_message",
        operator: :string_contains,
        value: "hello world",
        modifiers: %{quoted_string: true}
      ]

      result = FilterRule.build(params)

      assert %FilterRule{} = result
      assert result.path == "event_message"
      assert result.operator == :string_contains
      assert result.value == "hello world"
      assert result.modifiers == %{quoted_string: true}
    end

    test "builds result with range values from keyword list" do
      params = [
        path: "metadata.count",
        operator: :range,
        values: [10, 50]
      ]

      result = FilterRule.build(params)

      assert %FilterRule{} = result
      assert result.path == "metadata.count"
      assert result.operator == :range
      assert result.values == [10, 50]
    end

    test "builds result with shorthand from keyword list" do
      params = [
        path: "timestamp",
        operator: :range,
        shorthand: "today"
      ]

      result = FilterRule.build(params)

      assert %FilterRule{} = result
      assert result.path == "timestamp"
      assert result.operator == :range
      assert result.shorthand == "today"
    end

    test "returns empty struct for invalid changeset" do
      result = FilterRule.build([])
      assert %FilterRule{} = result
      assert result.path == nil
      assert result.operator == nil
    end
  end

  describe "virtual_fields/0" do
    test "returns virtual field names" do
      fields = FilterRule.virtual_fields()

      assert is_list(fields)
      assert :path in fields
      assert :value in fields
      assert :values in fields
      assert :operator in fields
      assert :modifiers in fields
      assert :shorthand in fields
    end
  end

  describe "Jason.Encoder" do
    test "encodes filter rule to JSON" do
      filter_rule = %FilterRule{
        path: "metadata.level",
        operator: :=,
        value: "info",
        modifiers: %{quoted_string: false}
      }

      json = Jason.encode!(filter_rule)
      decoded = Jason.decode!(json)

      assert decoded["path"] == "metadata.level"
      assert decoded["operator"] == "="
      assert decoded["value"] == "info"
      assert decoded["modifiers"] == %{"quoted_string" => false}
    end

    test "encodes filter rule with range values" do
      filter_rule = %FilterRule{
        path: "metadata.count",
        operator: :range,
        values: [1, 100]
      }

      json = Jason.encode!(filter_rule)
      decoded = Jason.decode!(json)

      assert decoded["path"] == "metadata.count"
      assert decoded["operator"] == "range"
      assert decoded["values"] == [1, 100]
      assert decoded["value"] == nil
    end
  end

  describe "rule-specific operations" do
    test "extract_timestamp_filters/1 extracts only timestamp rules" do
      rules = [
        %FilterRule{path: "timestamp", operator: :>, value: ~N[2020-01-01 00:00:00]},
        %FilterRule{path: "event_message", operator: :=, value: "error"},
        %FilterRule{
          path: "timestamp",
          operator: :range,
          values: [~N[2020-01-01 00:00:00], ~N[2020-01-02 00:00:00]]
        },
        %FilterRule{path: "metadata.level", operator: :=, value: "info"}
      ]

      timestamp_rules = FilterRule.extract_timestamp_filters(rules)

      assert length(timestamp_rules) == 2
      assert Enum.all?(timestamp_rules, &(&1.path == "timestamp"))
    end

    test "extract_metadata_filters/1 extracts non-timestamp rules" do
      rules = [
        %FilterRule{path: "timestamp", operator: :>, value: ~N[2020-01-01 00:00:00]},
        %FilterRule{path: "event_message", operator: :=, value: "error"},
        %FilterRule{path: "metadata.level", operator: :=, value: "info"}
      ]

      metadata_rules = FilterRule.extract_metadata_filters(rules)

      assert length(metadata_rules) == 2
      assert Enum.all?(metadata_rules, &(&1.path != "timestamp"))
    end

    test "is_shorthand_timestamp?/1 recognizes shorthand patterns" do
      # today/yesterday patterns
      today_rule = %FilterRule{shorthand: "today"}
      assert FilterRule.shorthand_timestamp?(today_rule) == true

      yesterday_rule = %FilterRule{shorthand: "yesterday"}
      assert FilterRule.shorthand_timestamp?(yesterday_rule) == true

      # last@/this@ patterns
      last_rule = %FilterRule{shorthand: "last@5minutes"}
      assert FilterRule.shorthand_timestamp?(last_rule) == true

      this_rule = %FilterRule{shorthand: "this@hour"}
      assert FilterRule.shorthand_timestamp?(this_rule) == true

      # non-shorthand patterns
      non_shorthand = %FilterRule{shorthand: "other"}
      assert FilterRule.shorthand_timestamp?(non_shorthand) == false

      nil_shorthand = %FilterRule{shorthand: nil}
      assert FilterRule.shorthand_timestamp?(nil_shorthand) == false
    end

    test "jump_timestamps/2 with empty timestamp list" do
      rules_without_timestamps = [
        %FilterRule{path: "event_message", operator: :=, value: "error"},
        %FilterRule{path: "metadata.level", operator: :=, value: "info"}
      ]

      result = FilterRule.jump_timestamps(rules_without_timestamps, :forwards)
      assert result == []

      result = FilterRule.jump_timestamps(rules_without_timestamps, :backwards)
      assert result == []
    end

    test "jump_timestamps/2 with timestamp rules" do
      rules_with_timestamps = [
        %FilterRule{path: "timestamp", operator: :>, value: ~N[2020-01-01 00:00:00]},
        %FilterRule{path: "timestamp", operator: :<=, value: ~N[2020-01-02 00:00:00]}
      ]

      result = FilterRule.jump_timestamps(rules_with_timestamps, :forwards)

      assert length(result) == 1
      [jump_rule] = result
      assert %FilterRule{} = jump_rule
      assert jump_rule.path == "timestamp"
      assert jump_rule.operator == :range
      assert is_list(jump_rule.values)
      assert length(jump_rule.values) == 2
    end
  end
end
