defmodule Logflare.Utils.Chart do
  @moduledoc """
  Utilities for chart-related calculations used in LQL validation and search operations.
  """

  @chart_periods [:second, :minute, :hour, :day]

  @doc """
  Calculates the number of chart ticks (time intervals) between two timestamps for a given period.

  This is used to validate that chart queries don't generate too many data points,
  which would make charts unusable or queries too expensive.

  ## Examples

      iex> start_time = ~U[2023-01-01 00:00:00Z]
      iex> end_time = ~U[2023-01-01 01:00:00Z]
      iex> Logflare.Utils.Chart.get_number_of_chart_ticks(start_time, end_time, :minute)
      60

      iex> start_date = ~D[2025-07-04]
      iex> end_date = ~D[2025-07-18]
      iex> Logflare.Utils.Chart.get_number_of_chart_ticks(start_date, end_date, :day)
      14
  """
  @spec get_number_of_chart_ticks(
          Date.t() | DateTime.t(),
          Date.t() | DateTime.t(),
          atom()
        ) :: pos_integer
  def get_number_of_chart_ticks(min, max, period) do
    Timex.diff(max, min, period)
  end

  @doc """
  Find the smallest valid period that produces chart ticks <= max_ticks.
  It tries periods in this order: :second, :minute, :hour, :day.

  Defaults to `:day`, even if that period exceeds `max_ticks`.

  ## Examples

      iex> start_time = ~U[2023-01-01 00:00:00Z]
      iex> end_time = ~U[2023-01-01 02:00:00Z]
      iex> Logflare.Utils.Chart.calculate_minimum_required_period(start_time, end_time, 150)
      :minute

      iex> start_time = ~U[2023-01-01 00:00:00Z]
      iex> end_time = ~U[2023-01-15 00:00:00Z]
      iex> Logflare.Utils.Chart.calculate_minimum_required_period(start_time, end_time, 1000)
      :hour

      iex> start_time = ~U[2020-01-01 00:00:00Z]
      iex> end_time = ~U[2023-01-01 00:00:00Z]
      iex> Logflare.Utils.Chart.calculate_minimum_required_period(start_time, end_time, 1000)
      :day

  """
  @spec calculate_minimum_required_period(DateTime.t(), DateTime.t(), pos_integer()) ::
          Logflare.Logs.SearchOperations.chart_period()
  def calculate_minimum_required_period(min_ts, max_ts, max_ticks) do
    Enum.reduce_while(@chart_periods, nil, fn period, _acc ->
      ticks = get_number_of_chart_ticks(min_ts, max_ts, period)

      if ticks <= max_ticks do
        {:halt, period}
      else
        {:cont, :day}
      end
    end)
  end
end
