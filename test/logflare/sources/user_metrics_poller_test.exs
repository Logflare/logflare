defmodule Logflare.Sources.UserMetricsPollerTest do
  use Logflare.DataCase, async: false
  alias Logflare.Sources.UserMetricsPoller

  def user_with_source(_) do
    _plan = insert(:plan)
    user = insert(:user)
    source = insert(:source, user: user)

    [user: user, source: source]
  end

  def user_metrics_poller(%{user: user}) do
    UserMetricsPoller.track(self(), user.id)

    # Wait for registration
    Process.sleep(100)

    poller_pid =
      case :syn.lookup(:core, {UserMetricsPoller, user.id}) do
        {poller_pid, _} ->
          poller_pid

        other ->
          other
      end

    on_exit(fn ->
      Logflare.Utils.Tasks.kill_all_tasks()
      ref = Process.monitor(poller_pid)
      Process.exit(poller_pid, :shutdown)
      Process.sleep(100)
      assert_receive {:DOWN, ^ref, _, _, _}
    end)

    [poller_pid: poller_pid]
  end

  def stub_rpc_multicall(%{user: user}) do
    Logflare.Cluster.Utils
    |> stub(:rpc_multicall, fn
      Logflare.PubSubRates.Cache, :get_all_local_metrics, [user_id] when user_id == user.id ->
        sources = Logflare.Sources.list_sources_by_user(user_id)

        node_metrics =
          Enum.reduce(sources, %{}, fn source, acc ->
            Map.put(acc, source.token, %{
              rates: %{average_rate: 10, last_rate: 5, max_rate: 15},
              buffer: %{len: 15},
              inserts: %{"node" => %{bq_inserts: 123, node_inserts: 456}}
            })
          end)

        {
          # two nodes rerurn results, zero bad nodes
          [node_metrics, node_metrics],
          []
        }
    end)

    :ok
  end

  describe "metrics polling and broadcasting" do
    setup [:user_with_source, :stub_rpc_multicall, :user_metrics_poller]

    test "receives metrics updates from UserMetricsPoller", %{
      user: user,
      source: source,
      poller_pid: poller_pid
    } do
      Phoenix.PubSub.subscribe(Logflare.PubSub, "dashboard_user_metrics:#{user.id}")

      send(poller_pid, :poll_metrics)

      assert_receive {:metrics_update, metrics}, 1000

      # two nodes return results, so all values are doubled
      assert Map.get(metrics, source.token) ==
               %{
                 avg: 10 * 2,
                 rate: 5 * 2,
                 max: 15 * 2,
                 buffer: 15 * 2,
                 inserts: 579 * 2
               }
    end

    test "tracks subscribers", %{user: user} do
      UserMetricsPoller.track(self(), 1)
      assert UserMetricsPoller.list_subscribers(user.id) |> length() == 1

      # Same process, so ignored as a duplicate
      UserMetricsPoller.track(self(), user.id)
      assert UserMetricsPoller.list_subscribers(user.id) |> length() == 1

      Task.async(fn ->
        # new process, new subscriber

        UserMetricsPoller.track(self(), user.id)
        assert UserMetricsPoller.list_subscribers(user.id) |> length() == 2
      end)
      |> Task.await()

      # different user_id
      another_user = insert(:user)
      UserMetricsPoller.track(self(), another_user.id)

      assert UserMetricsPoller.list_subscribers(another_user.id) |> length() == 1
    end

    test "shuts down when all subscribers unsubscribe", %{user: user, poller_pid: poller_pid} do
      ref = Process.monitor(poller_pid)

      UserMetricsPoller.untrack(self(), user.id)
      Process.sleep(100)

      assert UserMetricsPoller.list_subscribers(user.id) == []

      send(poller_pid, :poll_metrics)

      assert_receive {:DOWN, ^ref, _, _, :normal}, 5000

      refute Process.alive?(poller_pid)
    end
  end

  describe "broadcasting metrics in batches" do
    setup [:user_with_source]

    setup %{user: user} do
      # 55 sources in total
      sources = for _i <- 1..54, do: insert(:source, user: user)

      [user: user, sources: sources]
    end

    setup [:stub_rpc_multicall, :user_metrics_poller]

    test "batches large metrics updates", %{user: user, poller_pid: poller_pid} do
      Phoenix.PubSub.subscribe(Logflare.PubSub, "dashboard_user_metrics:#{user.id}")

      send(poller_pid, :poll_metrics)

      assert_receive {:metrics_update, first_batch}, 1000
      assert Enum.count(first_batch) == 50

      assert_receive {:metrics_update, second_batch}, 1000
      assert Enum.count(second_batch) == 5

      all_tokens = Map.keys(first_batch) ++ Map.keys(second_batch)
      expected_tokens = Logflare.Sources.list_sources_by_user(user.id) |> Enum.map(& &1.token)
      assert Enum.sort(all_tokens) == Enum.sort(expected_tokens)
    end
  end
end
