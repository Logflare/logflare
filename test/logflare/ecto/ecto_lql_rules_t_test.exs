defmodule Ecto.LqlRulesTest do
  use ExUnit.Case, async: true

  alias Ecto.LqlRules
  alias Logflare.Lql.Rules.ChartRule
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Lql.Rules.SelectRule

  describe "basic Ecto.Type functions" do
    test "type/0, cast/1, dump/1, embed_as/1, equal?/2" do
      assert LqlRules.type() == :binary
      assert {:ok, "test"} = LqlRules.cast("test")
      assert {:ok, nil} = LqlRules.dump(nil)
      assert {:ok, ""} = LqlRules.dump("")
      assert LqlRules.embed_as("any") == :self
      assert LqlRules.equal?("same", "same")
      refute LqlRules.equal?("", nil)
    end
  end

  describe "load/1" do
    test "handles edge cases and errors" do
      assert {:ok, nil} = LqlRules.load(nil)
      assert {:ok, ""} = LqlRules.load("")
      assert {:error, %ArgumentError{}} = LqlRules.load("invalid")
    end

    test "converts legacy `FilterRule` from actual error case" do
      legacy_filter_rule = %{
        value: "",
        values: nil,
        path: "event_message",
        operator: :"~",
        modifiers: %{quoted_string: true},
        __struct__: Logflare.Lql.FilterRule,
        shorthand: nil
      }

      {:ok, binary_data} = LqlRules.dump([legacy_filter_rule])
      {:ok, [converted_rule]} = LqlRules.load(binary_data)

      assert %FilterRule{
               path: "event_message",
               operator: :"~",
               value: "",
               values: nil,
               modifiers: %{quoted_string: true},
               shorthand: nil
             } = converted_rule
    end

    test "converts mixed legacy LQL rule types" do
      legacy_rules = [
        %{
          __struct__: Logflare.Lql.FilterRule,
          path: "metadata.status",
          operator: :=,
          value: "error",
          values: nil,
          modifiers: %{},
          shorthand: nil
        },
        %{
          __struct__: Logflare.Lql.ChartRule,
          aggregate: :count,
          period: :hour
        }
      ]

      {:ok, binary_data} = LqlRules.dump(legacy_rules)
      {:ok, [filter_rule, chart_rule]} = LqlRules.load(binary_data)

      assert %FilterRule{path: "metadata.status", operator: :=, value: "error"} = filter_rule
      assert %ChartRule{aggregate: :count, period: :hour} = chart_rule
    end

    test "handles current rules, plain maps, and mixed data" do
      current_filter_rule = %FilterRule{
        path: "test",
        operator: :=,
        value: "test",
        values: nil,
        modifiers: %{},
        shorthand: nil
      }

      current_select_rule = %SelectRule{
        path: "metadata.user_id",
        wildcard: false
      }

      plain_maps = [
        %{path: "level", operator: :=, value: "error"},
        %{aggregate: :sum, period: :day}
      ]

      mixed_data = [%{other: "data"}, "string", current_filter_rule, current_select_rule]

      {:ok, current_binary} = LqlRules.dump([current_filter_rule, current_select_rule])
      {:ok, [loaded_filter, loaded_select]} = LqlRules.load(current_binary)
      assert loaded_filter == current_filter_rule
      assert loaded_select == current_select_rule

      {:ok, maps_binary} = LqlRules.dump(plain_maps)
      {:ok, [filter_rule, chart_rule]} = LqlRules.load(maps_binary)
      assert %FilterRule{} = filter_rule
      assert %ChartRule{} = chart_rule

      {:ok, mixed_binary} = LqlRules.dump(mixed_data)

      {:ok, [other_map, string_val, loaded_filter_rule, loaded_select_rule]} =
        LqlRules.load(mixed_binary)

      assert other_map == %{other: "data"}
      assert string_val == "string"
      assert %FilterRule{} = loaded_filter_rule
      assert %SelectRule{} = loaded_select_rule
    end
  end
end
