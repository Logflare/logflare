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

    invalid_chart = Enum.find_value(chart_rules, &validate_chart_rule/1)

    cond do
      tailing? and not Enum.empty?(lql_ts_filters) ->
        @timestamp_filter_with_tailing

      ListUtils.at_least?(chart_rules, 2) ->
        "Only one chart rule can be used in a LQL query"

      invalid_chart ->
        invalid_chart

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

  @spec validate_chart_rule(ChartRule.t()) :: String.t() | nil
  def validate_chart_rule(%ChartRule{aggregate: aggregate, value_type: type, path: path})
      when aggregate in [:sum, :avg] and type not in [nil, :integer, :float] do
    "Chart aggregate '#{aggregate}' requires a numeric field, but path '#{path}' has type '#{inspect(type)}'."
  end

  def validate_chart_rule(%ChartRule{}), do: nil
end
