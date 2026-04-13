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
    filter =
      args
      |> Map.new()
      |> then(&struct!(FilterRule, &1))
      |> maybe_merge_value_modifiers()

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
            {values, modifiers} = unwrap_values_modifiers(v)

            %{
              filter
              | value: nil,
                values: values,
                operator: :range,
                modifiers: Map.merge(filter.modifiers, modifiers)
            }

          [[v]] ->
            {value, modifiers} = unwrap_value_modifiers(v)
            %{filter | value: value, modifiers: Map.merge(filter.modifiers, modifiers)}
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

  @spec parse_date_or_datetime_with_range(list()) ::
          [Date.t() | NaiveDateTime.t() | {:with_modifiers, NaiveDateTime.t(), map()}]
  def parse_date_or_datetime_with_range(result) when is_list(result) do
    explicit_timezone =
      Enum.any?(result, fn
        {:timezone, _timezone} -> true
        _ -> false
      end)

    [lv, rv] =
      result
      |> Enum.reduce([%{}, %{}], fn
        {:timezone, timezone}, [lacc, racc] ->
          [Map.put(lacc, :timezone, timezone), Map.put(racc, :timezone, timezone)]

        {k, v}, [lacc, racc] ->
          [lv, rv] =
            case v do
              [lv, rv] -> [lv, rv]
              [v] -> [v, v]
            end

          [Map.put(lacc, k, lv), Map.put(racc, k, rv)]
      end)
      |> Enum.map(&build_date_or_datetime_from_parts/1)
      |> maybe_mark_explicit_timezone(explicit_timezone)

    if lv == rv do
      [lv]
    else
      [lv, rv]
    end
  end

  @spec parse_date_or_datetime([{:date | :datetime, String.t()}]) :: Date.t() | NaiveDateTime.t()
  def parse_date_or_datetime([{tag, result}]), do: parse_iso8601_value!(tag, result)

  @spec parse_timestamp_datetime([{:datetime | :datetime_tz, String.t()}]) ::
          NaiveDateTime.t() | {:with_modifiers, NaiveDateTime.t(), map()}
  def parse_timestamp_datetime([{tag, result}]) when tag in [:datetime, :datetime_tz] do
    value = parse_iso8601_value!(:datetime, result)

    if tag == :datetime_tz do
      {:with_modifiers, value, %{explicit_timezone: true}}
    else
      value
    end
  end

  @spec parse_datetime_literal(term()) :: {:ok, Date.t() | NaiveDateTime.t()} | {:error, term()}
  def parse_datetime_literal(%Date{} = value), do: {:ok, value}
  def parse_datetime_literal(%NaiveDateTime{} = value), do: {:ok, value}

  def parse_datetime_literal(%DateTime{} = value) do
    {:ok, value |> DateTime.shift_zone!("Etc/UTC") |> DateTime.to_naive()}
  end

  def parse_datetime_literal(value) when is_integer(value) do
    value
    |> Integer.to_string()
    |> parse_unix_timestamp()
  end

  def parse_datetime_literal(value) when is_binary(value) do
    cond do
      String.match?(value, ~r/^\d+$/) ->
        parse_unix_timestamp(value)

      String.contains?(value, "T") ->
        parse_iso8601_datetime(value)

      true ->
        parse_iso8601_date(value)
    end
  end

  def parse_datetime_literal(_value), do: {:error, :invalid_format}

  @spec parse_unix_timestamp_literal([String.t()]) :: {:with_modifiers, NaiveDateTime.t(), map()}
  def parse_unix_timestamp_literal([value]) do
    case parse_unix_timestamp(value) do
      {:ok, parsed} ->
        {:with_modifiers, parsed, %{explicit_timezone: true}}

      {:error, :invalid_format} ->
        throw(
          "Error while parsing timestamp: '#{value}' is not a valid Unix timestamp (expected 10, 13, or 16 digits)"
        )

      {:error, error} ->
        throw(error)
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
      "Error while parsing timestamp filter value: expected ISO8601 string, Unix timestamp, range or shorthand, got '#{v}'"
    )
  end

  def check_for_no_invalid_metadata_field_values(
        %{path: p, value: {:invalid_metadata_field_value, v}},
        :metadata
      ) do
    throw("Error while parsing `#{p}` field metadata filter value: #{v}")
  end

  def check_for_no_invalid_metadata_field_values(rule, _), do: rule

  @spec check_for_invalid_blank_select_alias(Keyword.t() | {atom(), any()}) ::
          Keyword.t() | {atom(), any()} | no_return()
  def check_for_invalid_blank_select_alias({:invalid_blank_select_alias, "@"}) do
    throw(
      "Error while parsing select clause: select alias cannot be blank, expected alias name after @. For example, s:my_field@my_alias"
    )
  end

  def check_for_invalid_blank_select_alias(args), do: args

  defp build_date_or_datetime_from_parts(%{second: _, minute: _, hour: _} = dt) do
    {timezone, dt} = Map.pop(dt, :timezone)
    microseconds = Map.get(dt, :microsecond)

    if is_binary(timezone) do
      dt
      |> datetime_parts_to_iso8601(microseconds, timezone)
      |> then(&parse_iso8601_value!(:datetime, &1))
    else
      dt =
        Map.update(dt, :microsecond, {0, 0}, fn us ->
          {float, ""} = Float.parse("0." <> us)

          {round(float * 1_000_000), 6}
        end)

      struct!(NaiveDateTime, dt)
    end
  end

  defp build_date_or_datetime_from_parts(d), do: struct!(Date, d)

  defp datetime_parts_to_iso8601(dt, microseconds, timezone) do
    datetime =
      "#{dt.year}-#{pad_int(dt.month)}-#{pad_int(dt.day)}T#{pad_int(dt.hour)}:#{pad_int(dt.minute)}:#{pad_int(dt.second)}"

    case microseconds do
      nil ->
        datetime <> timezone

      us ->
        datetime <> "." <> us <> timezone
    end
  end

  defp pad_int(value), do: String.pad_leading(to_string(value), 2, "0")

  defp parse_iso8601_value!(:date, value) do
    case parse_iso8601_date(value) do
      {:ok, parsed} ->
        parsed

      {:error, :invalid_format} ->
        throw("Error while parsing timestamp date value: expected ISO8601 string, got '#{value}'")

      {:error, error} ->
        throw(error)
    end
  end

  defp parse_iso8601_value!(:datetime, value) do
    case parse_iso8601_datetime(value) do
      {:ok, parsed} ->
        parsed

      {:error, :invalid_format} ->
        throw(
          "Error while parsing timestamp datetime value: expected ISO8601 string, got '#{value}'"
        )

      {:error, error} ->
        throw(error)
    end
  end

  defp parse_iso8601_date(value) do
    case Date.from_iso8601(value) do
      {:ok, date} -> {:ok, date}
      {:error, :invalid_format} -> {:error, :invalid_format}
      {:error, error} -> {:error, error}
    end
  end

  defp parse_iso8601_datetime(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} ->
        {:ok, DateTime.to_naive(datetime)}

      {:error, :missing_offset} ->
        case NaiveDateTime.from_iso8601(value) do
          {:ok, datetime} -> {:ok, datetime}
          {:error, :invalid_format} -> {:error, :invalid_format}
          {:error, error} -> {:error, error}
        end

      {:error, :invalid_format} ->
        {:error, :invalid_format}

      {:error, error} ->
        {:error, error}
    end
  end

  defp parse_unix_timestamp(value) when is_binary(value) do
    case normalize_unix_timestamp(value) do
      {timestamp, unit} when is_integer(timestamp) ->
        case DateTime.from_unix(timestamp, unit) do
          {:ok, datetime} -> {:ok, DateTime.to_naive(datetime)}
          {:error, error} -> {:error, error}
        end

      :error ->
        {:error, :invalid_format}

      {:error, error} ->
        {:error, error}
    end
  end

  defp normalize_unix_timestamp(value) do
    case byte_size(value) do
      10 ->
        case Integer.parse(value) do
          {timestamp, ""} -> {timestamp, :second}
          _ -> :error
        end

      13 ->
        case Integer.parse(value) do
          {timestamp, ""} -> {timestamp, :millisecond}
          _ -> :error
        end

      16 ->
        case Integer.parse(value) do
          {timestamp, ""} -> {div(timestamp, 1_000), :millisecond}
          _ -> :error
        end

      _ ->
        {:error, :invalid_format}
    end
  end

  defp maybe_merge_value_modifiers(%FilterRule{} = rule) do
    {value, modifiers} = unwrap_value_modifiers(rule.value)

    %{
      rule
      | value: value,
        modifiers: Map.merge(rule.modifiers, modifiers)
    }
  end

  defp unwrap_value_modifiers({:with_modifiers, value, modifiers}),
    do: {value, modifiers}

  defp unwrap_value_modifiers({:range_operator, [lvalue, rvalue]}) do
    {lvalue, lmodifiers} = unwrap_value_modifiers(lvalue)
    {rvalue, rmodifiers} = unwrap_value_modifiers(rvalue)

    {{:range_operator, [lvalue, rvalue]}, Map.merge(lmodifiers, rmodifiers)}
  end

  defp unwrap_value_modifiers(value), do: {value, %{}}

  defp unwrap_values_modifiers(values) when is_list(values) do
    Enum.reduce(values, {[], %{}}, fn value, {acc, modifiers} ->
      {value, value_modifiers} = unwrap_value_modifiers(value)
      {[value | acc], Map.merge(modifiers, value_modifiers)}
    end)
    |> then(fn {values, modifiers} -> {Enum.reverse(values), modifiers} end)
  end

  defp maybe_mark_explicit_timezone(values, true) when is_list(values) do
    Enum.map(values, &{:with_modifiers, &1, %{explicit_timezone: true}})
  end

  defp maybe_mark_explicit_timezone(values, false), do: values
end
