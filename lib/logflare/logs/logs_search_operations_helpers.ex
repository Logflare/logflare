defmodule Logflare.Logs.SearchOperations.Helpers do
  @moduledoc false

  def default_min_max_timestamps_for_chart_period(chart_period) do
    case chart_period do
      :day ->
        {Timex.shift(Timex.now(), days: -30), Timex.now()}

      :hour ->
        {Timex.shift(Timex.now(), hours: -168), Timex.now()}

      :minute ->
        {Timex.shift(Timex.now(), minutes: -120), Timex.now()}

      :second ->
        {Timex.shift(Timex.now(), seconds: -180), Timex.now()}
    end
  end

  def min_max_timestamps(timestamps) do
    Enum.min_max_by(timestamps, &Timex.to_unix/1)
  end

  def override_min_max_for_open_intervals([%{operator: ">=", value: ts}]) do
    [ts, Timex.now()]
  end

  def override_min_max_for_open_intervals([%{operator: "<=", value: ts}]) do
    [Timex.now() |> Timex.shift(days: -30), ts]
  end

  def override_min_max_for_open_intervals(filter_rules) do
    Enum.map(filter_rules, & &1.value)
  end

  def convert_timestamp_timezone(row, user_timezone) do
    Map.update!(row, "timestamp", &Timex.Timezone.convert(&1, user_timezone))
  end

  def format_agg_row_keys(rows) do
    rows
    |> Enum.map(fn row ->
      row
      |> Enum.map(&agg_row_key_to_names/1)
      |> Map.new()
    end)
  end

  def format_agg_row_values(rows) do
    rows
    |> Enum.map(fn row ->
      row
      |> Enum.map(&agg_row_key_formatter/1)
      |> Map.new()
    end)
  end

  defp agg_row_key_to_names({"f0_", v}), do: {"timestamp", v}
  defp agg_row_key_to_names({"f1_", v}), do: {"value", v}

  defp agg_row_key_formatter({"timestamp", v}) do
    # {:ok, v} =
    #   v
    #   |> Timex.from_unix(:microseconds)
    #   |> Timex.format("{RFC822z}")

    {"timestamp", v}
  end

  defp agg_row_key_formatter(x), do: x
end
