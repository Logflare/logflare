defmodule Logflare.Utils.ChartTest do
  use ExUnit.Case, async: true

  alias Logflare.Utils.Chart
  doctest Chart

  describe "get_number_of_chart_ticks/3" do
    test "calculates correct number of ticks for minute intervals" do
      start_time = ~U[2023-03-16 00:00:00Z]
      end_time = ~U[2023-03-16 01:43:00Z]

      result = Chart.get_number_of_chart_ticks(start_time, end_time, :minute)

      assert result == 103
    end

    test "calculates correct number of ticks for hour intervals" do
      start_time = ~U[2023-04-01 11:44:00Z]
      end_time = ~U[2023-04-02 01:00:00Z]

      result = Chart.get_number_of_chart_ticks(start_time, end_time, :hour)

      assert result == 13
    end

    test "calculates correct number of ticks for day intervals" do
      start_date = ~D[2023-04-15]
      end_date = ~D[2024-03-08]

      result = Chart.get_number_of_chart_ticks(start_date, end_date, :day)

      assert result == 328
    end

    test "calculates correct number of ticks for second intervals" do
      start_time = ~U[2023-01-01 00:00:00Z]
      end_time = ~U[2023-01-01 00:01:23Z]

      result = Chart.get_number_of_chart_ticks(start_time, end_time, :second)

      assert result == 83
    end

    test "handles same start and end time" do
      start_time = ~U[2023-01-01 11:17:00Z]
      end_time = ~U[2023-01-01 11:17:00Z]

      result = Chart.get_number_of_chart_ticks(start_time, end_time, :minute)

      assert result == 0
    end

    test "handles fractional intervals" do
      start_time = ~U[2025-07-18 00:00:00Z]
      end_time = ~U[2025-07-18 00:00:30Z]

      result = Chart.get_number_of_chart_ticks(start_time, end_time, :minute)

      # Less than a minute
      assert result == 0
    end

    test "handles large time intervals" do
      start_date = ~D[2023-01-01]
      end_date = ~D[2025-07-18]

      result = Chart.get_number_of_chart_ticks(start_date, end_date, :day)

      assert result == 929
    end

    test "works with mixed date and datetime types" do
      start_date = ~D[2023-01-01]
      end_datetime = ~U[2023-01-02 12:21:09Z]

      result = Chart.get_number_of_chart_ticks(start_date, end_datetime, :hour)

      assert result == 36
    end
  end
end
