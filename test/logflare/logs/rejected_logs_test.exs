defmodule Logflare.Logs.RejectedEventsTest do
  @moduledoc false
  alias Logflare.Logs.RejectedEvents
  alias Logflare.{Sources, Source, Users, LogEvent}
  import Logflare.DummyFactory
  use Logflare.DataCase
  use Placebo

  setup do
    s1 = build(:source)
    s2 = build(:source)
    sources = [s1, s2]
    u1 = insert(:user, sources: sources)

    allow(Source.Data.get_rate()) |> exec(fn _ -> 0 end)
    allow(Source.Data.get_latest_date()) |> exec(fn _ -> 0 end)
    allow(Source.Data.get_avg_rate()) |> exec(fn _ -> 0 end)
    allow(Source.Data.get_max_rate()) |> exec(fn _ -> 0 end)
    allow(Source.Data.get_buffer()) |> exec(fn _ -> 0 end)
    allow(Source.Data.get_total_inserts()) |> exec(fn _ -> 0 end)

    {:ok, users: [u1], sources: sources}
  end

  describe "rejected logs module" do
    test "inserts logs for source and validator", %{sources: [s1, _]} do
      validator = Logflare.Logs.Validators.EqDeepFieldTypes

      source = Sources.get_by(token: s1.token)
      timestamp = System.system_time(:microsecond)

      log_event = %LogEvent{
        body: %{
          message: "test",
          metadata: %{
            "ip" => "0.0.0.0"
          },
          timestamp: timestamp
        },
        validation_error: validator.message(),
        source: source
      }

      _ = RejectedEvents.injest(log_event)

      cached = RejectedEvents.get_by_source(source)

      assert [%{message: validator_message, body: log_event, timestamp: _}] = cached
    end

    test "gets logs for all sources for user", %{users: [u1], sources: [s1, s2]} do
      source1 = Sources.get_by(token: s1.token)
      source2 = Sources.get_by(token: s2.token)
      _user = Users.get_by(id: u1.id)

      validator = Logflare.Logs.Validators.EqDeepFieldTypes
      timestamp = System.system_time(:microsecond)

      log_event_1_source_1 = %LogEvent{
        body: %{
          message: "case1",
          metadata: %{
            "ip" => "0.0.0.0"
          },
          timestamp: timestamp
        },
        source: s1,
        valid?: false,
        validation_error: validator.message()
      }

      log_event_2_source_1 = %LogEvent{
        body: %{
          message: "case2",
          metadata: %{
            "ip" => "0.0.0.0"
          },
          timestamp: timestamp
        },
        source: s1,
        valid?: false,
        validation_error: validator.message()
      }

      log_event_1_source_2 = %LogEvent{
        body: %{
          message: "case2",
          metadata: %{
            "ip" => "0.0.0.0"
          },
          timestamp: timestamp
        },
        source: s2,
        valid?: false,
        validation_error: validator.message()
      }

      _ = RejectedEvents.injest(log_event_1_source_1)
      _ = RejectedEvents.injest(log_event_2_source_1)
      _ = RejectedEvents.injest(log_event_1_source_2)

      result = RejectedEvents.get_by_user(u1)

      assert map_size(result) == 2

      assert [
               %{message: validator_message, body: raw_logs_source_1, timestamp: _},
               %{message: validator_message, body: raw_logs_source_1, timestamp: _}
             ] = result[s1.token]

      assert [
               %{message: validator_message, body: raw_logs_source_2, timestamp: _}
             ] = result[s2.token]
    end
  end
end
