defmodule Logflare.Logs.SearchOperations.Helpers do
  @moduledoc false

  alias Logflare.Logs.SearchOperations
  alias Logflare.Lql.Rules.FilterRule, as: FR

  @type minmax :: %{min: DateTime.t(), max: DateTime.t(), message: nil | String.t()}
  @default_open_interval_length 1_000

  @spec get_min_max_filter_timestamps([FR.t()], atom()) :: minmax()
  def get_min_max_filter_timestamps([], chart_period) do
    {min, max} = default_min_max_for_tailing_chart_period(chart_period)
    %{min: min, max: max, message: nil}
  end

  def get_min_max_filter_timestamps([%{operator: :range, values: [lvalue, rvalue]}], _) do
    %{min: lvalue, max: rvalue, message: nil}
  end

  def get_min_max_filter_timestamps([_tsf] = ts_filters, chart_period) do
    {min, max} =
      ts_filters
      |> override_min_max_for_open_intervals(chart_period)
      |> min_max_timestamps()

    message =
      case ts_filters do
        [%{operator: op}] when op in ~w[> >= < <=]a -> generate_message(chart_period)
        _ -> nil
      end

    %{min: min, max: max, message: message}
  end

  def get_min_max_filter_timestamps(ts_filters, _chart_period) when is_list(ts_filters) do
    {min, max} =
      ts_filters
      |> Enum.map(& &1.value)
      |> min_max_timestamps()

    %{min: min, max: max, message: nil}
  end

  def default_min_max_for_tailing_chart_period(period) when is_atom(period) do
    tick_count = default_period_tick_count(period)
    shift_key = to_timex_shift_key(period)

    from = Timex.shift(Timex.now(), [{shift_key, -tick_count + 1}])
    {from, Timex.now()}
  end

  def min_max_timestamps(timestamps) do
    Enum.min_max_by(timestamps, &Timex.to_unix/1)
  end

  @ops ~w[> >=]a
  defp override_min_max_for_open_intervals([%{operator: op, value: ts}], period)
       when op in @ops do
    shift = [{to_timex_shift_key(period), @default_open_interval_length}]

    max = ts |> Timex.shift(shift)

    max =
      if Timex.compare(max, Timex.now()) > 0 do
        Timex.now()
      else
        max
      end

    [ts, max]
  end

  @ops ~w[< <=]a
  defp override_min_max_for_open_intervals([%{operator: op, value: ts}], period)
       when op in @ops do
    shift = [{to_timex_shift_key(period), -@default_open_interval_length}]
    [ts |> Timex.shift(shift), ts]
  end

  defp override_min_max_for_open_intervals([%{operator: :=, value: ts}], period) do
    shift_key = to_timex_shift_key(period)
    [Timex.shift(ts, [{shift_key, -1}]), Timex.shift(ts, [{shift_key, 1}])]
  end

  @spec to_timex_shift_key(:day | :hour | :minute | :second) ::
          :days | :hours | :minutes | :seconds
  def to_timex_shift_key(period) do
    case period do
      :day -> :days
      :hour -> :hours
      :minute -> :minutes
      :second -> :seconds
    end
  end

  def default_period_tick_count(period) do
    case period do
      :day -> 31
      :hour -> 168
      :minute -> 120
      :second -> 180
    end
  end

  def convert_timestamp_timezone(row, user_timezone) do
    Map.update!(row, "timestamp", &Timex.Timezone.convert(&1, user_timezone))
  end

  @spec get_number_of_chart_ticks(
          Date.t() | DateTime.t(),
          Date.t() | DateTime.t(),
          SearchOperations.chart_period()
        ) :: pos_integer
  def get_number_of_chart_ticks(min, max, period) do
    Timex.diff(max, min, period)
  end

  def generate_message(period) do
    "Your timestamp filter is an unbounded interval. Max number of chart ticks is limited to #{@default_open_interval_length} #{to_timex_shift_key(period)}."
  end
end
