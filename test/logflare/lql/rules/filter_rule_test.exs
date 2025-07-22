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

      assert is_map(result)
    end

    test "builds result with modifiers from keyword list" do
      params = [
        path: "event_message",
        operator: :string_contains,
        value: "hello world",
        modifiers: %{quoted_string: true}
      ]

      result = FilterRule.build(params)

      assert is_map(result)
    end

    test "builds result with range values from keyword list" do
      params = [
        path: "metadata.count",
        operator: :range,
        values: [10, 50]
      ]

      result = FilterRule.build(params)

      assert is_map(result)
    end

    test "builds result with shorthand from keyword list" do
      params = [
        path: "timestamp",
        operator: :range,
        shorthand: "today"
      ]

      result = FilterRule.build(params)

      assert is_map(result)
    end
  end

  describe "fields/0" do
    test "returns schema fields" do
      fields = FilterRule.fields()

      assert is_list(fields) or is_atom(fields)
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
end
