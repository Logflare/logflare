defmodule Logflare.LogsTest do
  @moduledoc false
  use ExUnit.Case
  import Logflare.Logs

  describe "Logs context" do
    test "build_time_event/1" do
      now = NaiveDateTime.utc_now()

      iso_datetime =
        now
        |> Timex.to_datetime(Timex.Timezone.local())
        |> Timex.format!("{ISO:Extended}")

      {timestamp, unique_int, monotime} = build_time_event(iso_datetime)

      assert is_integer(timestamp)
    end

    test "validate_log_entry/2 succeeds with empty metadata" do
      le = %{
        "metadata" => %{},
        "message" => "param validation",
        "timestamp" => generate_timestamp_param(),
        "level" => "info"
      }

      {:ok, val} = validate_log_entry(le)
    end
  end

  def generate_timestamp_param() do
    NaiveDateTime.utc_now()
    |> Timex.to_datetime(Timex.Timezone.local())
    |> Timex.format!("{ISO:Extended}")
  end
end
