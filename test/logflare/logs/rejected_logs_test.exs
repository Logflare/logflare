defmodule Logflare.Logs.RejectedLogEventsTest do
  @moduledoc false
  use Logflare.Commons
  import Logflare.Factory
  use Logflare.DataCase
  @moduletag :unboxed

  setup do
    {:ok, u1} = Users.insert_or_update_user(params_for(:user))
    {:ok, s1} = Sources.create_source(params_for(:source), u1)
    {:ok, s2} = Sources.create_source(params_for(:source), u1)
    u1 = Users.preload_defaults(u1)

    {:ok, users: [u1], sources: [s1, s2]}
  end

  describe "rejected logs module" do
    test "inserts logs for source and validator", %{sources: [s1, _]} do
      validator = Logflare.Logs.Validators.EqDeepFieldTypes

      source = Sources.get_source_by(token: s1.token)
      timestamp = System.system_time(:microsecond)

      log_event = %LogEvent{
        params: %{
          "message" => "test",
          "metadata" => %{
            "ip" => "0.0.0.0"
          }
        },
        validation_error: validator.message(),
        source: source,
        ingested_at: timestamp,
        valid: false
      }

      _ = RejectedLogEvents.ingest(log_event)

      [rle] = RejectedLogEvents.get_for_source(source)

      assert rle.ingested_at == DateTime.from_unix!(timestamp, :microsecond)
      assert rle.params == log_event.params
    end

    test "gets logs for all sources for user", %{users: [u1], sources: [s1, s2]} do
      source1 = Sources.get_source_by(token: s1.token)
      source2 = Sources.get_source_by(token: s2.token)
      user = Users.get_by_and_preload(id: u1.id)

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
        source: source1,
        ingested_at: timestamp,
        valid: false,
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
        source: source1,
        ingested_at: timestamp,
        valid: false,
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
        source: source2,
        ingested_at: timestamp,
        valid: false,
        validation_error: validator.message()
      }

      _ = RejectedLogEvents.ingest(log_event_1_source_1)
      _ = RejectedLogEvents.ingest(log_event_2_source_1)
      _ = RejectedLogEvents.ingest(log_event_1_source_2)

      result = RejectedLogEvents.get_for_user(user)

      assert map_size(result) == 2

      assert [
               %RejectedLogEvent{
                 validation_error: validator_message,
                 params: _,
                 ingested_at: _
               },
               %RejectedLogEvent{
                 validation_error: validator_message,
                 params: _,
                 ingested_at: _
               }
             ] = result[source1.token]

      assert [
               %RejectedLogEvent{
                 validation_error: validator_message,
                 params: _,
                 ingested_at: _
               }
             ] = result[source2.token]
    end
  end
end
