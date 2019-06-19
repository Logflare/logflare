defmodule Logflare.LogsTest do
  @moduledoc false
  use Logflare.DataCase
  use Placebo
  import Logflare.DummyFactory
  alias Logflare.Logs
  alias Logflare.Logs.LogEvent, as: LE
  alias Logflare.Logs.{RejectedLogEvents}
  alias Logflare.{SystemMetrics, Source, Sources}
  alias Logflare.Source.{BigQuery.Buffer, RecentLogsServer}
  alias Logflare.Sources.Counters

  setup do
    u = insert(:user)
    [sink1, sink2] = insert_list(2, :source, user_id: u.id)
    rule1 = build(:rule, sink: sink1.token, regex: "pattern2")
    rule2 = build(:rule, sink: sink2.token, regex: "pattern3")
    s1 = insert(:source, token: Faker.UUID.v4(), rules: [rule1, rule2], user_id: u.id)

    {:ok, sources: [s1], sinks: [sink1, sink2]}
  end

  describe "log event injest for source with rules" do
    test "sink source routing", %{sources: [s1 | _], sinks: [sink1, sink2 | _]} do
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

      assert Logs.injest_logs(log_params_batch, s1) == :ok

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
                      is(fn le -> le.body.message === "pattern3" end)
                    ),
                    once()

      assert_called Sources.Counters.incriment(sink2.token), once()
      assert_called Sources.Counters.get_total_inserts(sink2.token), once()

      assert_called Buffer.push(
                      "#{sink2.token}",
                      is(fn le -> le.body.message === "pattern3" end)
                    ),
                    once()

      # All sources

      assert_called SystemMetrics.AllLogsLogged.incriment(any()), times(5)
    end

    @tag :run
    test "sink routing is allowed for one depth level only" do
      allow RecentLogsServer.push(any(), any()), return: :ok
      allow Buffer.push(any(), any()), return: :ok
      allow Sources.Counters.incriment(any()), return: {:ok, 1}
      allow SystemMetrics.AllLogsLogged.incriment(any()), return: :ok
      allow Counters.get_total_inserts(any()), return: {:ok, 1}

      u = insert(:user)

      s1 = insert(:source, rules: [], user_id: u.id)

      first_sink = insert(:source, user_id: u.id)

      last_sink = insert(:source, user_id: u.id)

      first_sink_rule =
        insert(:rule, sink: last_sink.token, regex: "test", source_id: first_sink.id)

      s1rule1 = insert(:rule, sink: first_sink.token, regex: "test", source_id: s1.id)

      log_params_batch = [
        %{"message" => "test"}
      ]

      s1 = Sources.get_by(id: s1.id)

      Logs.injest_logs(log_params_batch, s1) === :ok

      assert_called RecentLogsServer.push(s1.token, any), once()
      assert_called Sources.Counters.incriment(s1.token), once()
      assert_called Sources.Counters.get_total_inserts(s1.token), once()
      assert_called Buffer.push("#{s1.token}", any()), once()

      assert_called RecentLogsServer.push(first_sink.token, any), once()
      assert_called Sources.Counters.incriment(first_sink.token), once()
      assert_called Sources.Counters.get_total_inserts(first_sink.token), once()
      assert_called Buffer.push("#{first_sink.token}", any()), once()

      refute_called RecentLogsServer.push(last_sink.token, any), once()
      refute_called Sources.Counters.incriment(last_sink.token), once()
      refute_called Sources.Counters.get_total_inserts(last_sink.token), once()
      refute_called Buffer.push("#{last_sink.token}", any()), once()
    end
  end
end
