defmodule Logflare.Logs.RejectedLogEventsTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Logs.RejectedLogEvents
  alias Logflare.{Sources, Users, LogEvent}

  setup do
    insert(:plan)
    s1 = build(:source)
    s2 = build(:source)
    sources = [s1, s2]
    u1 = insert(:user, sources: sources)
    u1 = Users.preload_defaults(u1)

    {:ok, users: [u1], sources: sources}
  end

  describe "rejected logs module" do
    test "inserts logs for source and validator", %{sources: [s1, _]} do
      validator = Logflare.Logs.Validators.EqDeepFieldTypes

      source = Sources.get_by(token: s1.token)
      timestamp = System.system_time(:microsecond)

      log_event = %LogEvent{
        pipeline_error: %LogEvent.PipelineError{message: validator.message()},
        source: source,
        ingested_at: timestamp,
        valid: false
      }

      _ = RejectedLogEvents.ingest(log_event)

      [rle] = RejectedLogEvents.get_by_source(source)

      assert rle.ingested_at == timestamp
      assert rle.params == log_event.params
    end

    @tag :failing
    test "gets logs for all sources for user", %{users: [_u1], sources: [s1, s2]} do
      source1 = Sources.get_by(token: s1.token)
      source2 = Sources.get_by(token: s2.token)
      # user = Users.get_by_and_preload(id: u1.id)

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
        valid: false,
        pipeline_error: %LogEvent.PipelineError{message: validator.message()}
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
        valid: false,
        pipeline_error: %LogEvent.PipelineError{message: validator.message()}
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
        valid: false,
        pipeline_error: %LogEvent.PipelineError{message: validator.message()}
      }

      _ = RejectedLogEvents.ingest(log_event_1_source_1)
      _ = RejectedLogEvents.ingest(log_event_2_source_1)
      _ = RejectedLogEvents.ingest(log_event_1_source_2)

      # result = RejectedLogEvents.get_by_user(user)
      result = nil
      assert map_size(result) == 2

      assert [
               %LogEvent{
                 pipeline_error: %LogEvent.PipelineError{message: validator_message},
                 body: _,
                 ingested_at: _
               },
               %LogEvent{
                 pipeline_error: %LogEvent.PipelineError{message: validator_message},
                 body: _,
                 ingested_at: _
               }
             ] = result[source1.token]

      assert [
               %LogEvent{
                 pipeline_error: %LogEvent.PipelineError{message: ^validator_message},
                 body: _,
                 ingested_at: _
               }
             ] = result[source2.token]
    end
  end
end
