defmodule Logflare.Utils.Chart do
  @moduledoc """
  Utilities for chart-related calculations used in LQL validation and search operations.
  """

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
end
