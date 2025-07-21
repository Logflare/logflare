defmodule Logflare.Lql.Utils do
  @moduledoc false

  import Logflare.Utils.Guards

  alias Logflare.Lql.ChartRule
  alias Logflare.Lql.FilterRule

  @type lql_list :: [ChartRule.t() | FilterRule.t()]

  @spec default_chart_rule() :: ChartRule.t()
  def default_chart_rule() do
    %ChartRule{
      aggregate: :count,
      path: "timestamp",
      period: :minute,
      value_type: :datetime
    }
  end

  @spec get_chart_period(lql_list(), default :: any()) :: atom()
  def get_chart_period(lql_rules, default \\ nil) when is_list(lql_rules) do
    chart = get_chart_rule(lql_rules)

    if chart do
      chart.period
    else
      default
    end
  end

  @spec get_chart_aggregate(lql_list(), default :: any()) :: atom()
  def get_chart_aggregate(lql_rules, default \\ nil) when is_list(lql_rules) do
    chart = get_chart_rule(lql_rules)

    if chart do
      chart.aggregate
    else
      default
    end
  end

  @spec get_filter_rules(lql_list()) :: lql_list()
  def get_filter_rules(rules) when is_list(rules) do
    Enum.filter(rules, &match?(%FilterRule{}, &1))
  end

  @spec get_chart_rules(lql_list()) :: lql_list()
  def get_chart_rules(rules) when is_list(rules) do
    Enum.filter(rules, &match?(%ChartRule{}, &1))
  end

  @spec get_chart_rule(lql_list()) :: ChartRule.t() | nil
  def get_chart_rule(rules) when is_list(rules) do
    Enum.find(rules, &match?(%ChartRule{}, &1))
  end

  @spec update_timestamp_rules(lql_list(), lql_list()) :: lql_list()
  def update_timestamp_rules(lql_list, new_rules)
      when is_list(lql_list) and is_list(new_rules) do
    lql_list
    |> Enum.reject(&match?(%FilterRule{path: "timestamp"}, &1))
    |> Enum.concat(new_rules)
  end

  @spec put_chart_period(lql_list(), atom()) :: lql_list()
  def put_chart_period(lql_list, period) when is_list(lql_list) and is_atom_value(period) do
    i = Enum.find_index(lql_list, &match?(%ChartRule{}, &1))

    chart =
      lql_list
      |> Enum.at(i)
      |> Map.put(:period, period)

    List.replace_at(lql_list, i, chart)
  end

  @spec update_chart_rule(lql_list(), ChartRule.t(), params :: map()) :: lql_list()
  def update_chart_rule(rules, %ChartRule{} = default, params)
      when is_list(rules) and is_map(params) do
    i = Enum.find_index(rules, &match?(%ChartRule{}, &1))

    if i do
      chart =
        rules
        |> Enum.at(i)
        |> Map.merge(params)

      List.replace_at(rules, i, chart)
    else
      [default | rules]
    end
  end

  @spec put_new_chart_rule(lql_list(), ChartRule.t()) :: lql_list()
  def put_new_chart_rule(rules, %ChartRule{} = chart) when is_list(rules) do
    i = Enum.find_index(rules, &match?(%ChartRule{}, &1))

    if i do
      rules
    else
      [chart | rules]
    end
  end

  @spec get_ts_filters(lql_list()) :: lql_list()
  def get_ts_filters(rules) when is_list(rules) do
    Enum.filter(rules, &(&1.path == "timestamp"))
  end

  @spec get_meta_and_msg_filters(lql_list()) :: lql_list()
  def get_meta_and_msg_filters(rules) when is_list(rules) do
    Enum.filter(rules, &(&1.path != "timestamp"))
  end

  @spec get_lql_parser_warnings(lql_list(), Keyword.t()) :: String.t() | nil
  def get_lql_parser_warnings(lql_rules, dialect: :routing) when is_list(lql_rules) do
    cond do
      Enum.find(lql_rules, &(&1.path == "timestamp")) ->
        "Timestamp LQL clauses are ignored for event routing"

      Enum.find(lql_rules, &(&1.path == "timestamp")) ->
        "Timestamp LQL clauses are ignored for event routing"

      true ->
        nil
    end
  end

  @spec jump_timestamp(lql_list(), :backwards | :forwards) :: lql_list()
  def jump_timestamp(rules, direction)
      when is_list(rules) and direction in [:backwards, :forwards] do
    timestamp_rules =
      get_ts_filters(rules)
      |> get_filter_rules()

    timestamps =
      timestamp_rules
      |> Enum.map(&(&1.value || &1.values))
      |> List.flatten()

    from = Enum.min(timestamps)
    to = Enum.max(timestamps)

    diff =
      case direction do
        :forwards ->
          -NaiveDateTime.diff(from, to, :microsecond)

        :backwards ->
          NaiveDateTime.diff(from, to, :microsecond)
      end

    from = NaiveDateTime.add(from, diff, :microsecond)
    to = NaiveDateTime.add(to, diff, :microsecond)

    range = %Logflare.Lql.FilterRule{
      modifiers: %{},
      operator: :range,
      path: "timestamp",
      shorthand: nil,
      value: nil,
      values: [from, to]
    }

    update_timestamp_rules(rules, [range])
  end

  @spec timestamp_filter_rule_is_shorthand?(FilterRule.t()) :: boolean()
  def timestamp_filter_rule_is_shorthand?(%FilterRule{shorthand: shorthand}) do
    case shorthand do
      x when binary_part(x, 0, 4) in ["last", "this"] -> true
      x when x in ["today", "yesterday"] -> true
      _ -> false
    end
  end
end
