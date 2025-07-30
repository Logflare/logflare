defmodule Logflare.Backends.RecentInsertsBroadcaster do
  @moduledoc """
  Performs periodic broadcasting of cluster insert counts
  """
  use TypedStruct
  use GenServer

  alias Logflare.PubSubRates
  alias Logflare.Backends
  alias Logflare.Source
  alias Logflare.Sources.Counters

  require Logger

  @broadcast_every if Application.compile_env(:logflare, :env) == :test, do: 100, else: 5_000

  ## Server
  def start_link(args) do
    GenServer.start_link(__MODULE__, args,
      name: Backends.via_source(args[:source], __MODULE__),
      hibernate_after: 5_000,
      spawn_opt: [
        fullsweep_after: 100
      ]
    )
  end

  ## Client
  def init(args) do
    source = Keyword.get(args, :source)

    Process.flag(:trap_exit, true)
    Logger.metadata(source_id: source.token, source_token: source.token)
    broadcast()

    Logger.debug("[#{__MODULE__}] Started")

    {:ok,
     %{
       source_token: source.token,
       source_id: source.id
     }}
  end

  def handle_info(:broadcast, state) do
    {:ok, total_cluster_inserts, inserts_since_boot} = broadcast_count(state)

    prev_inserts_since_boot = Counters.get_inserts_since_boot(state.source_token)
    prev_last_cluster_inserts = Counters.get_total_cluster_inserts(state.source_token)
    prev_changed_at = Counters.get_source_changed_at_unix_ms(state.source_token)
    now = System.system_time(:microsecond)

    if prev_inserts_since_boot < inserts_since_boot do
      Counters.increment_inserts_since_boot_count(
        state.source_token,
        inserts_since_boot - prev_inserts_since_boot
      )

      Counters.increment_source_changed_at_unix_ts(
        state.source_token,
        now - prev_changed_at
      )
    end

    if prev_last_cluster_inserts < total_cluster_inserts do
      Counters.increment_total_cluster_inserts_count(
        state.source_token,
        total_cluster_inserts - prev_last_cluster_inserts
      )
    end

    broadcast()

    {:noreply, state}
  end

  def handle_info({:EXIT, _from, _reason}, state) do
    {:noreply, state}
  end

  def handle_info(message, state) do
    Logger.warning("[#{__MODULE__}] Unhandled message: #{inspect(message)}")

    {:noreply, state}
  end

  def terminate(reason, _state) do
    Logger.info("[#{__MODULE__}] Going Down: #{inspect(reason)}")
    reason
  end

  ## Private Functions

  defp broadcast_count(%{source_token: source_token}) do
    current_inserts = Source.Data.get_node_inserts(source_token)

    if current_inserts > Counters.get_inserts_since_boot(source_token) do
      bq_inserts = Source.Data.get_bq_inserts(source_token)
      inserts_payload = %{Node.self() => %{node_inserts: current_inserts, bq_inserts: bq_inserts}}

      PubSubRates.global_broadcast_rate({"inserts", source_token, inserts_payload})
    end

    current_cluster_inserts = PubSubRates.Cache.get_cluster_inserts(source_token)
    last_cluster_inserts = Counters.get_total_cluster_inserts(source_token)

    if current_cluster_inserts > last_cluster_inserts do
      payload = %{log_count: current_cluster_inserts, source_token: source_token}
      Source.ChannelTopics.local_broadcast_log_count(payload)
    end

    {:ok, current_cluster_inserts, current_inserts}
  end

  defp broadcast() do
    # scale broadcasting interval to cluster size
    cluster_size = Logflare.Cluster.Utils.actual_cluster_size()
    broadcast_every = max(@broadcast_every, round(:rand.uniform(cluster_size * 200)))
    Process.send_after(self(), :broadcast, broadcast_every)
  end
end
