defmodule Logflare.Lql.Parser.RuleBuilders do
  @moduledoc """
  Functions for building `FilterRule` structs from parsed data
  """

  alias Logflare.Lql.FilterRule

  @isolated_string :isolated_string

  @level_orders %{
    0 => "debug",
    1 => "info",
    2 => "notice",
    3 => "warning",
    4 => "error",
    5 => "critical",
    6 => "alert",
    7 => "emergency"
  }

  @spec to_rule(Keyword.t()) :: [FilterRule.t()]
  def to_rule(metadata_level_clause: ["metadata.level", {:range_operator, [left, right]}]) do
    left..right
    |> Enum.map(&Map.get(@level_orders, &1))
    |> Enum.map(fn level ->
      %FilterRule{
        path: "metadata.level",
        operator: :=,
        value: level,
        modifiers: %{}
      }
    end)
  end

  @spec to_rule(Keyword.t(), atom()) :: FilterRule.t() | {:quoted, String.t()}
  def to_rule(args, :quoted_field_value) do
    {:quoted, args[@isolated_string]}
  end

  def to_rule(args, :quoted_event_message) do
    %FilterRule{
      path: "event_message",
      value: args[@isolated_string],
      operator: args[:operator] || :string_contains,
      modifiers: %{quoted_string: true}
    }
  end

  def to_rule(args, :event_message) do
    %FilterRule{
      path: "event_message",
      value: args[:word],
      operator: args[:operator] || :string_contains
    }
  end

  def to_rule(args, :filter_maybe_shorthand) do
    args =
      case args[:value] do
        %{shorthand: sh, value: value} ->
          sh =
            case sh do
              "this@" <> _ -> String.trim_trailing(sh, "s")
              "last@" <> _ -> String.trim_trailing(sh, "s")
              _ -> sh
            end

          args
          |> Keyword.replace!(:value, value)
          |> Keyword.put(:shorthand, sh)

        _ ->
          args
      end

    to_rule(args, :filter)
  end

  def to_rule(args, :filter) when is_list(args) do
    filter = struct!(FilterRule, Map.new(args))

    cond do
      match?({:quoted, _}, filter.value) ->
        {:quoted, value} = filter.value

        filter
        |> Map.update!(:modifiers, &Map.put(&1, :quoted_string, true))
        |> Map.put(:value, value)

      match?(%{value: {:datetime_with_range, _}}, filter) ->
        {_, value} = filter.value

        case value do
          [[_, _] = v] ->
            %{filter | value: nil, values: v, operator: :range}

          [[v]] ->
            %{filter | value: v}
        end

      true ->
        filter
    end
  end

  @spec apply_value_modifiers([FilterRule.t()]) :: FilterRule.t()
  def apply_value_modifiers([rule]) do
    case rule.value do
      {:range_operator, [lvalue, rvalue]} ->
        lvalue =
          case lvalue do
            {:datetime_with_range, [[x]]} -> x
            x -> x
          end

        rvalue =
          case rvalue do
            {:datetime_with_range, [[x]]} -> x
            x -> x
          end

        %{rule | values: [lvalue, rvalue], operator: :range, value: nil}

      _ ->
        rule
    end
  end

  @spec maybe_apply_negation_modifier(term()) :: [FilterRule.t()] | FilterRule.t()
  def maybe_apply_negation_modifier([:negate, rules]) when is_list(rules) do
    Enum.map(rules, &maybe_apply_negation_modifier([:negate, &1]))
  end

  def maybe_apply_negation_modifier([:negate, rule]) do
    update_in(rule.modifiers, &Map.put(&1, :negate, true))
  end

  def maybe_apply_negation_modifier(rule), do: rule

  @spec get_level_order(String.t()) :: non_neg_integer() | nil
  def get_level_order(level) do
    @level_orders
    |> Enum.map(fn {k, v} ->
      {v, k}
    end)
    |> Map.new()
    |> Map.get(level)
  end
end
