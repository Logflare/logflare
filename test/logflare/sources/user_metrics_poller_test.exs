defmodule Logflare.Sources.UserMetricsPollerTest do
  use Logflare.DataCase, async: false
  alias Logflare.Sources.UserMetricsPoller

  setup do
    _plan = insert(:plan)
    user = insert(:user)
    source = insert(:source, user: user)
    [user: user, source: source]
  end

  describe "metrics polling and broadcasting" do
    setup do
      Logflare.Cluster.Utils
      |> stub(:rpc_multicall, fn
        Logflare.PubSubRates.Cache, :get_local_buffer, [_source_id, nil] ->
          {[%{len: 15}, %{len: 5}], []}

        Logflare.PubSubRates.Cache, :get_local_rates, [_source_id] ->
          {
            [
              %{average_rate: 10, last_rate: 5, max_rate: 15},
              %{average_rate: 10, last_rate: 5, max_rate: 15}
            ],
            []
          }

        Logflare.PubSubRates.Cache, :get_inserts, [_source_token] ->
          {[
             {:ok, %{"node" => %{bq_inserts: 123, node_inserts: 456}}}
           ], []}
      end)

      :ok
    end

    test "receives metrics updates from UserMetricsPoller", %{user: user, source: source} do
      UserMetricsPoller.track(self(), user.id)
      poller_pid = start_supervised!(UserMetricsPoller.child_spec(user.id))

      Phoenix.PubSub.subscribe(Logflare.PubSub, "dashboard_user_metrics:#{user.id}")

      send(poller_pid, :poll_metrics)

      assert_receive {:metrics_update, metrics}, 1000

      assert Map.get(metrics, source.token) ==
               %{
                 avg: 20,
                 rate: 10,
                 max: 30,
                 buffer: 20,
                 inserts: 579
               }
    end

    test "tracks subscribers" do
      UserMetricsPoller.track(self(), 1)
      assert UserMetricsPoller.list_subscribers(1) |> length() == 1

      # Same process, so ignored as a duplicate
      UserMetricsPoller.track(self(), 1)
      assert UserMetricsPoller.list_subscribers(1) |> length() == 1

      Task.async(fn ->
        # new process, new subscriber

        UserMetricsPoller.track(self(), 1)
        assert UserMetricsPoller.list_subscribers(1) |> length() == 2
      end)
      |> Task.await()

      # different user_id
      UserMetricsPoller.track(self(), 2)

      assert UserMetricsPoller.list_subscribers(2) |> length() == 1
    end

    test "shuts down when all subscribers unsubscribe", %{user: user} do
      UserMetricsPoller.track(self(), user.id)
      poller_pid = start_supervised!(UserMetricsPoller.child_spec(user.id))
      ref = Process.monitor(poller_pid)

      UserMetricsPoller.untrack(self(), user.id)
      assert UserMetricsPoller.list_subscribers(user.id) == []

      send(poller_pid, :poll_metrics)

      assert_receive {:DOWN, ^ref, _, _, :normal}, 5000

      refute Process.alive?(poller_pid)
    end

    test "refreshes sources", %{user: user} do
      UserMetricsPoller.track(self(), user.id)
      poller_pid = start_supervised!(UserMetricsPoller.child_spec(user.id))
      Phoenix.PubSub.subscribe(Logflare.PubSub, "dashboard_user_metrics:#{user.id}")

      send(poller_pid, :poll_metrics)

      assert_receive {:metrics_update, metrics}, 1000

      assert Map.keys(metrics) ==
               Logflare.Sources.list_sources_by_user(user.id) |> Enum.map(& &1.token)

      _new_source = insert(:source, user: user)
      send(poller_pid, :refresh_sources)
      send(poller_pid, :poll_metrics)

      assert_receive {:metrics_update, metrics}, 1000

      assert Map.keys(metrics) ==
               Logflare.Sources.list_sources_by_user(user.id) |> Enum.map(& &1.token)
    end
  end
end
