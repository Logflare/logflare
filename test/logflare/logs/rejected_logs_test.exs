defmodule Logflare.Logs.RejectedEventsTest do
  @moduledoc false
  alias Logflare.Logs.RejectedEvents
  alias Logflare.{Sources}
  import Logflare.DummyFactory
  use Logflare.DataCase

  setup do
    s1 = insert(:source)
    u1 = insert(:user, api_key: @api_key, sources: [s1])
    {:ok, users: [u1], sources: [s1]}
  end

  describe "rejected logs module" do
    test "inserts logs for source and error", %{sources: [s1]} do
      source = Sources.get_by_id(s1.token)

      raw_logs = [
        %{"log_entry" => "test", "metadata" => %{"ip" => "0.0.0.0"}},
        %{
          "log_entry" => "test",
          "metadata" => %{"ip" => %{"version" => 4, "address" => "0.0.0.0"}}
        }
      ]

      error = Logflare.Validator.DeepFieldTypes

      _ = RejectedEvents.insert(source, error, raw_logs)
      cached = RejectedEvents.get_by_source(source)

      assert cached[error] === raw_logs
    end
  end
end
