defmodule Logflare.Backends.RecentInsertsCacher do
  @moduledoc """
  Performs periodic broadcasting of cluster insert counts
  """
  use TypedStruct
  use GenServer

  alias Logflare.PubSubRates
  alias Logflare.Backends
  alias Logflare.Sources.Source
  alias Logflare.Sources.Counters

  require Logger

  @cache_every if Application.compile_env(:logflare, :env) == :test, do: 100, else: 5_000

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
    schedule_cache()

    Logger.debug("[#{__MODULE__}] Started")

    {:ok,
     %{
       source_token: source.token,
       source_id: source.id
     }}
  end

  def handle_info(:do_cache, state) do
    {:ok, total_cluster_inserts, inserts_since_boot} = cache_count(state)

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

    schedule_cache()

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
    Logger.debug("[#{__MODULE__}] Going Down: #{inspect(reason)}")
    reason
  end

  ## Private Functions

  defp cache_count(%{source_token: source_token}) do
    current_inserts = Source.Data.get_node_inserts(source_token)

    if current_inserts > Counters.get_inserts_since_boot(source_token) do
      bq_inserts = Source.Data.get_bq_inserts(source_token)
      inserts_payload = %{Node.self() => %{node_inserts: current_inserts, bq_inserts: bq_inserts}}

      PubSubRates.Cache.cache_inserts(source_token, inserts_payload)
    end

    current_cluster_inserts = PubSubRates.Cache.get_cluster_inserts(source_token)

    {:ok, current_cluster_inserts, current_inserts}
  end

  defp schedule_cache do
    Process.send_after(self(), :do_cache, @cache_every + :rand.uniform(1000))
  end
end
