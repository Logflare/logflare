defmodule Logflare.LogsTest do
  @moduledoc false
  use Logflare.DataCase
  use Placebo
  import Logflare.DummyFactory
  import Logflare.Logs
  alias Logflare.Logs.LogEvent, as: LE
  alias Logflare.Logs.{RejectedLogEvents}
  alias Logflare.{SystemMetrics, Source, Sources}
  alias Logflare.Source.{BigQuery.Buffer, RecentLogsServer}
  alias Logflare.Sources.Counters

  setup do
    u = insert(:user)
    [sink1, sink2] = insert_list(2, :source, user_id: u.id)
    rule1 = build(:rule, sink: sink1.token, regex: "\w+1")
    rule2 = build(:rule, sink: sink2.token, regex: "\w+2")
    s1 = insert(:source, token: Faker.UUID.v4(), rules: [rule1, rule2], user_id: u.id)

    {:ok, sources: [s1], sinks: [sink1, sink2]}
  end

  describe "log event injest for source with rules" do
    test "injest log event", %{sources: [s1 | _], sinks: [sink1, sink2 | _]} do
      allow RecentLogsServer.push(any(), any()), return: :ok
      allow Buffer.push(any(), any()), return: :ok
      allow Sources.Counters.incriment(any()), return: {:ok, 1}
      allow SystemMetrics.AllLogsLogged.incriment(any()), return: :ok
      allow Counters.get_total_inserts(any()), return: {:ok, 1}

      log_params_batch = [
        %{"message" => "pattern"},
        %{"message" => "pattern2"},
        %{"message" => "pattern3"}
      ]

      assert injest_logs(log_params_batch, s1) == :ok

      # Original source
      assert_called RecentLogsServer.push(s1.token, any), times(3)
      assert_called Sources.Counters.incriment(s1.token), times(3)
      assert_called Sources.Counters.get_total_inserts(s1.token), times(3)
      assert_called Buffer.push("#{s1.token}", any()), times(3)

      # Sink 1
      assert_called RecentLogsServer.push(
                      sink1.token,
                      is(fn le -> le.body.message === "pattern2" end)
                    ),
                    once()

      assert_called Sources.Counters.incriment(sink1.token), once()
      assert_called Sources.Counters.get_total_inserts(sink2.token), once()

      assert_called Buffer.push(
                      "#{sink1.token}",
                      is(fn le -> le.body.message === "pattern2" end)
                    ),
                    once()

      # Sink 2

      assert_called RecentLogsServer.push(
                      sink2.token,
                      is(fn le -> le.body.message === "pattern2" end)
                    ),
                    once()

      assert_called Sources.Counters.incriment(sink2.token), once()
      assert_called Sources.Counters.get_total_inserts(sink2.token), once()

      assert_called Buffer.push(
                      "#{sink2.token}",
                      is(fn le -> le.body.message === "pattern2" end)
                    ),
                    once()

      # All sources

      assert_called SystemMetrics.AllLogsLogged.incriment(any()), times(5)
    end
  end
end
