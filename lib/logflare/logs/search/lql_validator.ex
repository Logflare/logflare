defmodule Logflare.Lql.Validator do
  @moduledoc false
  import Logflare.Logs.SearchOperations.Helpers

  @timestamp_filter_with_tailing "Timestamp filters can't be used if live tail search is active"
  @default_max_n_chart_ticks 250

  def validate(lql_rules, opts) do
    %{
      lql_ts_filters: lql_ts_filters,
      chart_period: chart_period,
      chart_rules: chart_rules
    } = lql_rules

    %{tailing?: tailing?} = Map.new(opts)

    %{min: min_ts, max: max_ts} = get_min_max_filter_timestamps(lql_ts_filters, chart_period)

    cond do
      tailing? and not Enum.empty?(lql_ts_filters) ->
        @timestamp_filter_with_tailing

      length(chart_rules) > 1 ->
        "Only one chart rule can be used in a LQL query"

      match?([_], chart_rules) and
        hd(chart_rules).value_type not in ~w[integer float]a and
          hd(chart_rules).path != "timestamp" ->
        chart_rule = hd(chart_rules)

        "Can't aggregate on a non-numeric field type '#{chart_rule.value_type}' for path #{
          chart_rule.path
        }. Check the source schema for the field used with chart operator."

      Timex.diff(max_ts, min_ts, chart_period) == 0 ->
        "Selected chart period #{chart_period} is longer than the timestamp filter interval. Please select a shorter chart period."

      get_number_of_chart_ticks(min_ts, max_ts, chart_period) > @default_max_n_chart_ticks ->
        "The interval length between min and max timestamp is larger than #{
          @default_max_n_chart_ticks
        } periods, please use a longer chart aggregation period."

      true ->
        nil
    end
  end
end
