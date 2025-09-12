defmodule Logflare.Sources.UserMetricsPoller do
  use GenServer
  alias Logflare.Cluster.Utils
  alias Logflare.PubSubRates.Cache
  alias Logflare.Sources

  @poll_interval :timer.seconds(5)

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

  @doc "Subscribe a dashboard LiveView to receive metrics updates"
  def subscribe(user_id, dashboard_pid \\ self()) do
    GenServer.call(via_tuple(user_id), {:subscribe, dashboard_pid})
  end

  @doc "Unsubscribe a dashboard from metrics updates"
  def unsubscribe(user_id, dashboard_pid \\ self()) do
    GenServer.call(via_tuple(user_id), {:unsubscribe, dashboard_pid})
  end

  defp via_tuple(user_id) do
    {:via, :syn, {:user_metrics_poller, user_id}}
  end

  # Server Implementation
  def init(user_id) do
    schedule_poll()
    {:ok, %{user_id: user_id, subscribers: MapSet.new(), source_ids: []}}
  end

  def handle_call({:subscribe, pid}, _from, state) do
    Process.monitor(pid)
    new_subscribers = MapSet.put(state.subscribers, pid)

    # Update source_ids when first subscriber joins
    source_ids =
      if MapSet.size(state.subscribers) == 0 do
        Sources.list_sources_by_user(state.user_id) |> Enum.map(& &1.id)
      else
        state.source_ids
      end

    {:reply, :ok, %{state | subscribers: new_subscribers, source_ids: source_ids}}
  end

  def handle_call({:unsubscribe, pid}, _from, state) do
    new_subscribers = MapSet.delete(state.subscribers, pid)

    # Stop polling if no subscribers
    if MapSet.size(new_subscribers) == 0 do
      {:stop, :normal, :ok, state}
    else
      {:reply, :ok, %{state | subscribers: new_subscribers}}
    end
  end

  def handle_info(:poll_metrics, state) do
    if MapSet.size(state.subscribers) > 0 do
      metrics = fetch_cluster_metrics(state.source_ids)
      broadcast_to_subscribers(state.subscribers, {:metrics_update, metrics})
      schedule_poll()
    end

    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    new_subscribers = MapSet.delete(state.subscribers, pid)

    # Stop if no more subscribers
    if MapSet.size(new_subscribers) == 0 do
      {:stop, :normal, state}
    else
      {:noreply, %{state | subscribers: new_subscribers}}
    end
  end

  defp schedule_poll do
    Process.send_after(self(), :poll_metrics, @poll_interval)
  end

  defp fetch_cluster_metrics(source_ids) do
    Enum.map(source_ids, fn source_id ->
      source_atom = String.to_existing_atom("source_#{source_id}")

      {results, _bad_nodes} =
        Utils.rpc_multicall(
          Logflare.PubSubRates.Cache,
          :get_rates,
          [source_atom],
          5_000
        )

      nodes = Utils.node_list_all()
      node_metrics = Enum.zip(nodes, results) |> Enum.into(%{})
      {source_id, node_metrics}
    end)
    |> Enum.into(%{})
  end

  defp broadcast_to_subscribers(subscribers, message) do
    Enum.each(subscribers, fn pid ->
      send(pid, message)
    end)
  end
end
