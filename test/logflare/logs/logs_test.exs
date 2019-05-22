defmodule Logflare.LogsTest do
  @moduledoc false
  use ExUnit.Case
  import Logflare.Logs

  describe "Logs context" do
    test "build_time_event/1 for ISO:Extended" do
      now = NaiveDateTime.utc_now()

      iso_datetime =
        now
        |> Timex.to_datetime(Timex.Timezone.local())
        |> Timex.format!("{ISO:Extended}")

      {timestamp, unique_int, monotime} = build_time_event(iso_datetime)

      assert is_integer(timestamp)
    end

    test "build_time_event/1 for integer timestamp" do
      timestamp = System.system_time(:microsecond)

      {timestamp, unique_int, monotime} = build_time_event(now_timestamp)

      assert is_integer(timestamp)
    end

    test "validate_log_entry/1 succeeds with empty metadata" do
      le = %{
        "metadata" => %{},
        "message" => "param validation",
        "timestamp" => generate_timestamp_param(),
        "level" => "info"
      }

      {:ok, val} = validate_log_entry(le)
    end

    test "validate_log_entry/1 succeeds with simple valid metadata" do
      le = %{
        "metadata" => %{
          "ip" => "8.8.8.8",
          "host" => "example.org",
          "user" => %{
            "id" => 1,
            "sources" => [%{
              "id" => 1
            }, %{
              "id" => 2
            }]
          }
        },
        "message" => "param validation",
        "timestamp" => generate_timestamp_param(),
        "level" => "info"
      }

      {:ok, val} = validate_log_entry(le)
    end

    test "validate_all/1 succeeds with empty metadata log entries " do
      xs = [
        %{
          "metadata" => %{},
          "message" => "param validation",
          "timestamp" => generate_timestamp_param(),
          "level" => "info"
        },
        %{
          "metadata" => %{},
          "message" => "param validation",
          "timestamp" => generate_timestamp_param(),
          "level" => "info"
        }
      ]

      assert validate_all(xs) === :ok
    end
  end

  def generate_timestamp_param() do
    NaiveDateTime.utc_now()
    |> Timex.to_datetime(Timex.Timezone.local())
    |> Timex.format!("{ISO:Extended}")
  end
end
