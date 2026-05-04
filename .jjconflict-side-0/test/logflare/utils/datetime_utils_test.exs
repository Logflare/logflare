defmodule Logflare.DateTimeUtilsTest do
  use ExUnit.Case, async: true

  alias Logflare.DateTimeUtils
  doctest DateTimeUtils

  describe "truncate/2 with DateTime" do
    @datetime ~U[2024-03-15 14:35:45.123456Z]

    test "truncates DateTime for all precisions" do
      cases = [
        month: ~U[2024-03-01 00:00:00Z],
        day: ~U[2024-03-15 00:00:00Z],
        hour: ~U[2024-03-15 14:00:00Z],
        minute: ~U[2024-03-15 14:35:00Z],
        second: ~U[2024-03-15 14:35:45Z],
        millisecond: ~U[2024-03-15 14:35:45.123Z],
        microsecond: ~U[2024-03-15 14:35:45.123456Z]
      ]

      for {precision, expected} <- cases do
        assert DateTimeUtils.truncate(@datetime, precision) == expected
      end
    end

    test "preserves timezone" do
      result = DateTimeUtils.truncate(@datetime, :hour)
      assert result.time_zone == "Etc/UTC"
    end
  end

  describe "truncate/2 with NaiveDateTime" do
    @nativedatime ~N[2024-03-15 14:35:45.123456]

    test "truncates NaiveDateTime for all precisions" do
      cases = [
        month: ~N[2024-03-01 00:00:00],
        day: ~N[2024-03-15 00:00:00],
        hour: ~N[2024-03-15 14:00:00],
        minute: ~N[2024-03-15 14:35:00],
        second: ~N[2024-03-15 14:35:45],
        millisecond: ~N[2024-03-15 14:35:45.123],
        microsecond: ~N[2024-03-15 14:35:45.123456]
      ]

      for {precision, expected} <- cases do
        assert DateTimeUtils.truncate(@nativedatime, precision) == expected
      end
    end
  end
end
