defmodule Logflare.Logs.RejectedEventsTest do
  @moduledoc false
  alias Logflare.Logs.RejectedEvents
  alias Logflare.{Sources, Users}
  import Logflare.DummyFactory
  use Logflare.DataCase

  setup do
    s1 = insert(:source)
    s2 = insert(:source)
    sources = [s1, s2]
    u1 = insert(:user, api_key: @api_key, sources: sources)
    {:ok, users: [u1], sources: sources}
  end

  describe "rejected logs module" do
    test "inserts logs for source and validator", %{sources: [s1, _]} do
      source = Sources.get_by_id(s1.token)

      raw_logs = [
        %{"log_entry" => "test", "metadata" => %{"ip" => "0.0.0.0"}},
        %{
          "log_entry" => "test",
          "metadata" => %{"ip" => %{"version" => 4, "address" => "0.0.0.0"}}
        }
      ]

      validator = Logflare.Validator.DeepFieldTypes
      validator_message = validator.message()

      _ = RejectedEvents.injest(%{source: source, error: validator, batch: raw_logs})

      cached = RejectedEvents.get_by_source(source)

      assert [%{message: validator_message, payload: raw_logs, timestamp: _}] = cached
    end

    test "gets logs for all sources for user", %{users: [u1], sources: [s1, s2]} do
      source1 = Sources.get_by_id(s1.token)
      source2 = Sources.get_by_id(s2.token)
      user = Users.get_user_by_id(u1.id)

      raw_logs_source_1 = [
        %{"log_entry" => "case1", "metadata" => %{"ip" => "0.0.0.0"}},
        %{
          "log_entry" => "case1",
          "metadata" => %{"ip" => %{"version" => 4, "address" => "0.0.0.0"}}
        }
      ]

      raw_logs_source_2 = [
        %{"log_entry" => "case2", "metadata" => %{"ip" => "0.0.0.0"}},
        %{
          "log_entry" => "case2",
          "metadata" => %{"ip" => %{"version" => 4, "address" => "0.0.0.0"}}
        }
      ]

      validator = Logflare.Validator.DeepFieldTypes
      validator_message = validator.message()

      _ = RejectedEvents.injest(%{source: source1, error: validator, batch: raw_logs_source_1})
      _ = RejectedEvents.injest(%{source: source1, error: validator, batch: raw_logs_source_1})
      _ = RejectedEvents.injest(%{source: source2, error: validator, batch: raw_logs_source_2})

      result = RejectedEvents.get_by_user(u1)

      assert map_size(result) == 2

      assert [
               %{message: validator_message, payload: raw_logs_source_1, timestamp: _},
               %{message: validator_message, payload: raw_logs_source_1, timestamp: _}
             ] = result[s1.token]

      assert [
               %{message: validator_message, payload: raw_logs_source_2, timestamp: _}
             ] = result[s2.token]
    end
  end
end
