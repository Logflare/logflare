defmodule Logflare.Sources.UserMetricsPoller do
  use GenServer

  alias Logflare.Cluster
  alias Logflare.Sources

  @poll_interval :timer.seconds(5)
  @refresh_interval :timer.seconds(60)
  @batch_size 50

  @moduledoc """
  Polls and broadcasts source metrics for consumption by DashboardLive components.

  When a dashboard is mounted, it calls `subscribe_to_updates/2` to start
  metrics polling for a specific user. Each UserMetricsPoller instance is
  globally unique per user_id and automatically shuts down when there are no
  longer any active dashboards tracking the monitored user_id.

  ## Usage

  - Dashboards subscribe via `track/2` which starts the poller, if not already
    started, and tracks the dashboard
  - Metrics are polled at regular intervals and broadcast via PubSub
  - Dashboards must also subscribe to updates via PubSub to receive updates
  - The poller automatically stops when all dashboards disconnect or unmount
  - The list of sources is refreshed periodically to ensure sources created
    after the poller is started receives updates.

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

  def track(subscriber_pid, user_id) do
    {:ok, _pid} =
      Logflare.GenSingleton.start_link(
        child_spec: __MODULE__.child_spec(user_id),
        restart: :transient
      )

    Phoenix.Tracker.track(
      Logflare.ActiveUserTracker,
      subscriber_pid,
      channel(user_id),
      user_id,
      %{}
    )
  end

  def untrack(subscriber_pid, user_id) do
    Phoenix.Tracker.untrack(Logflare.ActiveUserTracker, subscriber_pid, channel(user_id), user_id)
  end

  defp via_tuple(user_id) do
    {:via, :syn, {:core, {__MODULE__, user_id}}}
  end

  # Server Implementation
  def init(user_id) do
    sources = get_sources(user_id)

    schedule_poll()
    schedule_sources_refresh()

    {:ok, %{user_id: user_id, sources: sources}}
  end

  @spec get_sources(non_neg_integer()) :: list(map)
  def get_sources(user_id) do
    Sources.list_sources_by_user(user_id) |> Enum.map(&Map.take(&1, [:id, :token]))
  end

  def list_subscribers(user_id) do
    Phoenix.Tracker.list(Logflare.ActiveUserTracker, channel(user_id))
  end

  def handle_info(:poll_metrics, state) do
    if Enum.any?(list_subscribers(state.user_id)) do
      {node_results, _bad_nodes} = fetch_cluster_metrics(state.user_id)
      aggregated_metrics = aggregate_all_metrics(node_results)
      broadcast_batched(state.user_id, aggregated_metrics)

      schedule_poll()
      {:noreply, state}
    else
      {:stop, :normal, state}
    end
  end

  def handle_info(:refresh_sources, state) do
    sources = get_sources(state.user_id)

    schedule_sources_refresh()
    {:noreply, %{state | sources: sources}}
  end

  defp schedule_sources_refresh do
    Process.send_after(self(), :refresh_sources, @refresh_interval)
  end

  defp schedule_poll do
    Process.send_after(self(), :poll_metrics, @poll_interval)
  end

  def fetch_cluster_metrics(user_id) do
    Cluster.Utils.rpc_multicall(Logflare.PubSubRates.Cache, :get_all_local_metrics, [user_id])
  end

  defp aggregate_all_metrics(node_results) do
    node_results
    |> Enum.flat_map(&Map.keys/1)
    |> Enum.uniq()
    |> Enum.reduce(%{}, fn source_token, acc ->
      source_metrics_from_nodes =
        node_results
        |> Enum.map(&Map.get(&1, source_token))
        |> Enum.reject(&is_nil/1)

      if Enum.any?(source_metrics_from_nodes) do
        rates = Enum.map(source_metrics_from_nodes, & &1.rates)
        buffers = Enum.map(source_metrics_from_nodes, & &1.buffer)
        inserts = Enum.map(source_metrics_from_nodes, & &1.inserts)

        aggregated =
          aggregate_rates(rates)
          |> Map.put(:buffer, aggregate_buffers(buffers))
          |> Map.put(:inserts, aggregate_inserts(inserts))

        Map.put(acc, source_token, aggregated)
      else
        acc
      end
    end)
  end

  defp broadcast_batched(user_id, metrics) do
    metrics
    |> Enum.chunk_every(@batch_size)
    |> Enum.each(fn batch ->
      batch_map = Enum.into(batch, %{})
      broadcast(user_id, {:metrics_update, batch_map})
    end)
  end

  defp aggregate_rates(node_results) do
    node_results
    |> Enum.reduce(%{avg: 0, rate: 0, max: 0}, fn
      %{average_rate: avg, last_rate: rate, max_rate: max}, acc ->
        %{
          avg: acc.avg + avg,
          rate: acc.rate + rate,
          max: acc.max + max
        }

      _, acc ->
        acc
    end)
  end

  defp aggregate_buffers(buffer_results) do
    Enum.reduce(buffer_results, 0, fn buffer, acc ->
      acc + Map.get(buffer, :len, 0)
    end)
  end

  defp aggregate_inserts(inserts_results) do
    inserts_results
    |> Enum.reduce(0, fn
      inserts_map, acc when is_map(inserts_map) ->
        Enum.reduce(inserts_map, acc, fn
          {_, %{bq_inserts: bq, node_inserts: node}}, node_acc ->
            node_acc + bq + node
        end)

      nil, acc ->
        acc
    end)
  end

  defp broadcast(user_id, message) do
    Phoenix.PubSub.broadcast(Logflare.PubSub, channel(user_id), message)
  end

  defp channel(user_id), do: "dashboard_user_metrics:#{user_id}"
end
