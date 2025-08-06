defmodule Logflare.Lql.Parser.Helpers do
  @moduledoc """
  Supporting functions for LQL parsing.

  This module contains all the helper functions that support the parsing process,
  including rule building, value casting, datetime parsing, and validation.
  """

  alias Logflare.DateTimeUtils
  alias Logflare.Lql.Rules.FilterRule

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

  # ============================================================================
  # Rule Building Functions
  # ============================================================================

  @spec to_rule(Keyword.t()) :: [FilterRule.t()]
  def to_rule(metadata_level_clause: ["metadata.level", {:range_operator, [left, right]}]) do
    left..right
    |> Enum.map(&Map.get(@level_orders, &1))
    |> Enum.map(fn level ->
      FilterRule.build(
        path: "metadata.level",
        operator: :=,
        value: level,
        modifiers: %{}
      )
    end)
  end

  @spec to_rule(Keyword.t(), atom()) :: FilterRule.t() | {:quoted, String.t()}
  def to_rule(args, :quoted_field_value) do
    {:quoted, args[@isolated_string]}
  end

  def to_rule(args, :quoted_event_message) do
    FilterRule.build(
      path: "event_message",
      value: args[@isolated_string],
      operator: args[:operator] || :string_contains,
      modifiers: %{quoted_string: true}
    )
  end

  def to_rule(args, :event_message) do
    FilterRule.build(
      path: "event_message",
      value: args[:word],
      operator: args[:operator] || :string_contains
    )
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

  # ============================================================================
  # DateTime Helper Functions
  # ============================================================================

  @spec parse_date_or_datetime_with_range(list()) :: [Date.t() | NaiveDateTime.t()]
  def parse_date_or_datetime_with_range(result) when is_list(result) do
    [lv, rv] =
      result
      |> Enum.reduce([%{}, %{}], fn
        {k, v}, [lacc, racc] ->
          [lv, rv] =
            case v do
              [lv, rv] -> [lv, rv]
              [v] -> [v, v]
            end

          [Map.put(lacc, k, lv), Map.put(racc, k, rv)]
      end)
      |> Enum.map(fn
        %{second: _, minute: _, hour: _} = dt ->
          dt =
            Map.update(dt, :microsecond, {0, 0}, fn us ->
              {float, ""} = Float.parse("0." <> us)

              {round(float * 1_000_000), 6}
            end)

          struct!(NaiveDateTime, dt)

        d ->
          struct!(Date, d)
      end)

    if lv == rv do
      [lv]
    else
      [lv, rv]
    end
  end

  @spec parse_date_or_datetime([{:date | :datetime, String.t()}]) :: Date.t() | NaiveDateTime.t()
  def parse_date_or_datetime([{tag, result}]) do
    mod =
      case tag do
        :date -> Date
        :datetime -> NaiveDateTime
      end

    case mod.from_iso8601(result) do
      {:ok, dt, _offset} ->
        dt

      {:ok, dt} ->
        dt

      {:error, :invalid_format} ->
        throw(
          "Error while parsing timestamp #{tag} value: expected ISO8601 string, got '#{result}'"
        )

      {:error, e} ->
        throw(e)
    end
  end

  @spec timestamp_shorthand_to_value([String.t() | atom()]) :: %{
          shorthand: String.t(),
          value: term()
        }
  def timestamp_shorthand_to_value(["now"]) do
    %{value: %{Timex.now() | microsecond: {0, 0}}, shorthand: "now"}
  end

  def timestamp_shorthand_to_value(["today"]) do
    dt = Timex.today() |> Timex.to_datetime()
    value = {:range_operator, [dt, Timex.shift(dt, days: 1, seconds: -1)]}

    %{value: value, shorthand: "today"}
  end

  def timestamp_shorthand_to_value(["yesterday"]) do
    dt = Timex.today() |> Timex.shift(days: -1) |> Timex.to_datetime()
    value = {:range_operator, [dt, Timex.shift(dt, days: 1, seconds: -1)]}

    %{value: value, shorthand: "yesterday"}
  end

  def timestamp_shorthand_to_value(["this", period]) do
    now_ndt = DateTimeUtils.truncate(Timex.now(), :second)
    today_ndt = DateTimeUtils.truncate(now_ndt, :day)

    lvalue =
      case period do
        :minutes ->
          DateTimeUtils.truncate(now_ndt, :minute)

        :hours ->
          DateTimeUtils.truncate(now_ndt, :hour)

        :days ->
          today_ndt

        :weeks ->
          Timex.beginning_of_week(today_ndt)

        :months ->
          Timex.beginning_of_month(today_ndt)

        :years ->
          Timex.beginning_of_year(today_ndt)
      end

    value = {:range_operator, [lvalue, now_ndt]}
    %{value: value, shorthand: "this@#{period}"}
  end

  def timestamp_shorthand_to_value(["last", amount, period]) do
    amount = -amount

    now_ndt = DateTimeUtils.truncate(Timex.now(), :second)

    truncated =
      case period do
        :seconds ->
          now_ndt

        :minutes ->
          DateTimeUtils.truncate(now_ndt, :minute)

        :hours ->
          DateTimeUtils.truncate(now_ndt, :hour)

        :days ->
          DateTimeUtils.truncate(now_ndt, :day)

        :weeks ->
          DateTimeUtils.truncate(now_ndt, :day)

        :months ->
          DateTimeUtils.truncate(now_ndt, :day)

        :years ->
          DateTimeUtils.truncate(now_ndt, :day)
      end

    lvalue = Timex.shift(truncated, [{period, amount}])
    value = {:range_operator, [lvalue, now_ndt]}
    %{value: value, shorthand: "last@#{if amount < 0, do: -amount, else: amount}#{period}"}
  end

  # ============================================================================
  # Validation Functions
  # ============================================================================

  @spec check_for_no_invalid_metadata_field_values(map(), :timestamp | :metadata) ::
          map() | no_return()
  def check_for_no_invalid_metadata_field_values(
        %{path: _p, value: {:invalid_metadata_field_value, v}},
        :timestamp
      ) do
    throw(
      "Error while parsing timestamp filter value: expected ISO8601 string or range or shorthand, got '#{v}'"
    )
  end

  def check_for_no_invalid_metadata_field_values(
        %{path: p, value: {:invalid_metadata_field_value, v}},
        :metadata
      ) do
    throw("Error while parsing `#{p}` field metadata filter value: #{v}")
  end

  def check_for_no_invalid_metadata_field_values(rule, _), do: rule
end
