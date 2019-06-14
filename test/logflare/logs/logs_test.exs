defmodule Logflare.LogsTest do
  @moduledoc false
  use Logflare.DataCase
  import Logflare.DummyFactory
  import Logflare.Logs
  alias Logflare.Logs.LogEvent

  setup do
    u = insert(:user)
    s = insert(:source, token: Faker.UUID.v4(), rules: [], user_id: u.id)

    {:ok, sources: [s]}
  end

  describe "Logs" do
    test "build_time_event/1 for ISO:Extended" do
      now = NaiveDateTime.utc_now()

      iso_datetime =
        now
        |> Timex.to_datetime(Timex.Timezone.local())
        |> Timex.format!("{ISO:Extended}")

      {timestamp, unique_int, monotime} = build_time_event(iso_datetime)

      assert is_integer(timestamp)
      assert is_integer(unique_int)
      assert is_integer(monotime)
    end

    test "build_time_event/1 for integer timestamp" do
      timestamp = System.system_time(:microsecond)

      {timestamp, unique_int, monotime} = build_time_event(timestamp)

      assert is_integer(timestamp)
      assert is_integer(unique_int)
      assert is_integer(monotime)
    end

  end

  def generate_timestamp_param() do
    NaiveDateTime.utc_now()
    |> Timex.to_datetime(Timex.Timezone.local())
    |> Timex.format!("{ISO:Extended}")
  end
end
