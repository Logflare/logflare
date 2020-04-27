defmodule Logflare.Lql.Utils do
  @moduledoc false
  alias Logflare.Lql.{FilterRule, ChartRule}
  @type lql_list :: [ChartRule.t() | FilterRule.t()]

  def default_chart_rule() do
    %ChartRule{
      aggregate: :count,
      path: "timestamp",
      period: :minute,
      value_type: :datetime
    }
  end

  def get_chart_period(lql_rules, default \\ nil) do
    chart = get_chart_rule(lql_rules)

    if chart do
      chart.period
    else
      default
    end
  end

  def get_chart_aggregate(lql_rules, default \\ nil) do
    chart = get_chart_rule(lql_rules)

    if chart do
      chart.aggregate
    else
      default
    end
  end

  def get_filter_rules(rules) do
    Enum.filter(rules, &match?(%FilterRule{}, &1))
  end

  def get_chart_rules(rules) do
    Enum.filter(rules, &match?(%ChartRule{}, &1))
  end

  @spec get_chart_rule(lql_list) :: ChartRule.t() | nil
  def get_chart_rule(rules) do
    Enum.find(rules, &match?(%ChartRule{}, &1))
  end

  def update_timestamp_rules(lql_list, new_rules) do
    lql_list
    |> Enum.reject(&match?(%FilterRule{path: "timestamp"}, &1))
    |> Enum.concat(new_rules)
  end

  def put_chart_period(lql_list, period) when is_atom(period) do
    i = Enum.find_index(lql_list, &match?(%ChartRule{}, &1))

    chart =
      lql_list
      |> Enum.at(i)
      |> Map.put(:period, period)

    List.replace_at(lql_list, i, chart)
  end

  @spec update_chart_rule(lql_list, ChartRule.t(), map()) :: lql_list
  def update_chart_rule(rules, default, params) when is_map(params) and is_list(rules) do
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

  def put_new_chart_rule(rules, chart) do
    i = Enum.find_index(rules, &match?(%ChartRule{}, &1))

    if i do
      rules
    else
      [chart | rules]
    end
  end

  def get_ts_filters(rules) do
    Enum.filter(rules, &(&1.path == "timestamp"))
  end

  def get_meta_and_msg_filters(rules) do
    Enum.filter(rules, &(&1.path != "timestamp"))
  end

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
end
