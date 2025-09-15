defmodule Logflare.Sources.UserMetricsPoller do
  use GenServer
  alias Logflare.Cluster.Utils
  alias Logflare.PubSubRates.Cache
  alias Logflare.Sources

  @poll_interval :timer.seconds(5)

  @moduledoc """
  Polls and broadcasts source metrics for consumption by DashboardLive components.

  When a dashboard is mounted, it calls `subscribe_to_updates/2` to start metrics polling for a specific user.
  Each UserMetricsPoller instance is globally unique per user_id and automatically shuts down when there are
  no longer any active dashboards tracking the monitored user_id.

  ## Usage

  - Dashboards subscribe via `subscribe_to_updates/2` which starts the poller, if not already started, and tracks the dashboard
  - Metrics are polled at regular intervals and broadcast via PubSub
  - The poller automatically stops when all dashboards disconnect or unmount
  - Uses Phoenix.Tracker to monitor active dashboard subscriptions

  """

  # Client API
  def start_link(user_id) when is_integer(user_id) do
    GenServer.start_link(__MODULE__, user_id, name: via_tuple(user_id))
  end

  def child_spec(user_id) do
    %{
      id: {__MODULE__, user_id},
      start: {__MODULE__, :start_link, [user_id]},
      restart: :transient
    }
  end

  def subscribe_to_updates(pid, user_id) do
    track(pid, user_id)

    {:ok, poller_pid} =
      Logflare.GenSingleton.start_link(
        child_spec: Logflare.Sources.UserMetricsPoller.child_spec(user_id)
      )

    {:ok, poller_pid}
  end

  def track(pid, user_id) do
    Phoenix.Tracker.track(
      Logflare.ActiveUserTracker,
      pid,
      tracker_channel(user_id),
      user_id,
      %{}
    )
  end

  def untrack(pid, user_id) do
    Phoenix.Tracker.untrack(
      Logflare.ActiveUserTracker,
      pid,
      tracker_channel(user_id),
      user_id
    )
  end

  defp via_tuple(user_id) do
    {:via, :syn, {:core, user_id}}
  end

  # Server Implementation
  def init(user_id) do
    source_ids = Sources.list_sources_by_user(user_id) |> Enum.map(& &1.token)
    # send(self(), :poll_metrics)

    {:ok, %{user_id: user_id, source_ids: source_ids}}
  end

  def list_subscribers(user_id) do
    Phoenix.Tracker.list(Logflare.ActiveUserTracker, tracker_channel(user_id))
  end

  def handle_info(:poll_metrics, state) do
    if Enum.any?(list_subscribers(state.user_id)) do
      metrics = fetch_cluster_metrics(state.source_ids)

      broadcast(state.user_id, {:metrics_update, metrics})
      schedule_poll()
      {:noreply, state}
    else
      {:stop, :normal, state}
    end
  end

  defp schedule_poll do
    Process.send_after(self(), :poll_metrics, @poll_interval)
  end

  defp fetch_cluster_metrics(source_ids) do
    Enum.map(source_ids, fn source_id ->
      {results, _bad_nodes} =
        Utils.rpc_multicall(Logflare.PubSubRates.Cache, :get_local_rates, [source_id])

      aggregated_metrics = aggregate_node_metrics(results)
      {source_id, aggregated_metrics}
    end)
    |> Enum.into(%{})
  end

  defp aggregate_node_metrics(node_results) do
    valid_results =
      Enum.filter(node_results, fn
        %{average_rate: _, last_rate: _, max_rate: _} -> true
        _ -> false
      end)

    case valid_results do
      [] ->
        %{
          average_rate: 0,
          last_rate: 0,
          max_rate: 0,
          limiter_metrics: %{average: 0, duration: 60, sum: 0}
        }

      metrics_list ->
        %{
          average_rate: sum_field(metrics_list, :average_rate),
          last_rate: sum_field(metrics_list, :last_rate),
          max_rate: sum_field(metrics_list, :max_rate)
        }
    end
  end

  defp sum_field(metrics_list, field) do
    Enum.reduce(metrics_list, 0, fn metrics, acc ->
      acc + Map.get(metrics, field, 0)
    end)
  end

  defp broadcast(user_id, message) do
    Phoenix.PubSub.broadcast(Logflare.PubSub, "user_metrics:#{user_id}", message)
  end

  defp tracker_channel(user_id), do: "dashboard_user_metrics:#{user_id}"
end
