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
  end
end
