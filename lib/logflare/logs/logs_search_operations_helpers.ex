defmodule Logflare.Logs.SearchOperations.Helpers do
  @moduledoc false
  alias Logflare.Lql.FilterRule

  def get_min_max_filter_timestamps(timestamp_filter_rules, chart_period) do
    if Enum.empty?(timestamp_filter_rules) do
      default_min_max_for_tailing_chart_period(chart_period)
    else
      timestamp_filter_rules
      |> override_min_max_for_open_intervals(chart_period)
      |> min_max_timestamps()
    end
  end

  def default_min_max_for_tailing_chart_period(period) when is_atom(period) do
    shift_interval =
      case period do
        :day -> [days: -31 + 1]
        :hour -> [hours: -168 + 1]
        :minute -> [minutes: -120 + 1]
        :second -> [seconds: -180 + 1]
      end

    {Timex.shift(Timex.now(), shift_interval), Timex.now()}
  end

  def min_max_timestamps(timestamps) do
    Enum.min_max_by(timestamps, &Timex.to_unix/1)
  end

  defp override_min_max_for_open_intervals([%{operator: op, value: ts}], period)
       when op in ~w[> >=]a do
    shift =
      case period do
        :day -> [days: 365]
        :hour -> [hours: 480]
        :minute -> [minutes: 360]
        :second -> [seconds: 300]
      end

    max = ts |> Timex.shift(shift)

    max =
      if Timex.compare(max, Timex.now()) > 0 do
        Timex.now()
      else
        max
      end

    [ts, max]
  end

  defp override_min_max_for_open_intervals([%{operator: op, value: ts}], period)
       when op in ~w[< <=]a do
    shift =
      case period do
        :day -> [days: -365]
        :hour -> [hours: -480]
        :minute -> [minutes: -360]
        :second -> [seconds: -300]
      end

    [ts |> Timex.shift(shift), ts]
  end

  defp override_min_max_for_open_intervals(filter_rules, _) do
    Enum.map(filter_rules, & &1.value)
  end

  def convert_timestamp_timezone(row, user_timezone) do
    Map.update!(row, "timestamp", &Timex.Timezone.convert(&1, user_timezone))
  end
end
