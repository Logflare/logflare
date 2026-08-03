defmodule Logflare.Lql.Validator do
  @moduledoc false

  alias Logflare.Logs.SearchOperations.Helpers, as: SearchOperationHelpers
  alias Logflare.Lql.Rules.ChartRule
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Lql.Rules.SelectRule
  alias Logflare.Utils.Chart
  alias Logflare.Utils.List, as: ListUtils

  @timestamp_filter_with_tailing "Timestamp filters can't be used if live tail search is active"
  @default_max_n_chart_ticks 250
  @max_select_rules 50

  @type lql_rules :: %{
          lql_ts_filters: [FilterRule.t()],
          chart_period: atom(),
          chart_rules: [ChartRule.t()],
          select_rules: [SelectRule.t()]
        }

  @spec validate(lql_rules(), opts :: Keyword.t()) :: String.t() | nil
  def validate(lql_rules, opts \\ []) when is_list(opts) do
    %{
      lql_ts_filters: lql_ts_filters,
      chart_period: chart_period,
      chart_rules: chart_rules,
      select_rules: select_rules
    } = lql_rules

    tailing? = Keyword.get(opts, :tailing?, false)

    tailing? =
      if is_boolean(tailing?) do
        tailing?
      else
        raise ArgumentError, "tailing? must be a boolean value"
      end

    %{min: min_ts, max: max_ts} =
      SearchOperationHelpers.get_min_max_filter_timestamps(lql_ts_filters, chart_period)

    cond do
      tailing? and not Enum.empty?(lql_ts_filters) ->
        @timestamp_filter_with_tailing

      ListUtils.at_least?(chart_rules, 2) ->
        "Only one chart rule can be used in a LQL query"

      ListUtils.exactly?(chart_rules, 1) and
        hd(chart_rules).value_type not in ~w[integer float]a and
          hd(chart_rules).path != "timestamp" ->
        chart_rule = hd(chart_rules)

        "Can't aggregate on a non-numeric field type '#{chart_rule.value_type}' for path #{chart_rule.path}. Check the source schema for the field used with chart operator."

      Timex.diff(max_ts, min_ts, chart_period) == 0 ->
        "Selected chart period #{chart_period} is longer than the timestamp filter interval. Please select a shorter chart period."

      Chart.get_number_of_chart_ticks(min_ts, max_ts, chart_period) > @default_max_n_chart_ticks ->
        "The interval length between min and max timestamp is larger than #{@default_max_n_chart_ticks} periods, please use a longer chart aggregation period."

      length(select_rules) > @max_select_rules ->
        "Too many field selections (maximum #{@max_select_rules} allowed)"

      true ->
        nil
    end
  end
end
