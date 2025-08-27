defmodule Logflare.ContextCache.CacheBusterWorker do
  @moduledoc """
    Monitors our Postgres replication log and busts the cache accordingly.
  """

  use GenServer

  require Logger

  alias Logflare.Alerting
  alias Logflare.Backends
  alias Logflare.Rules
  alias Logflare.Utils
  alias Logflare.ContextCache

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args)
  end

  def init(state) do
    {:ok, state}
  end

  def handle_cast({:to_bust, context_pkeys}, state) do
    ContextCache.bust_keys(context_pkeys)

    for record <- context_pkeys do
      maybe_do_cross_cluster_syncing(record)
    end

    {:noreply, state}
  end

  defp maybe_do_cross_cluster_syncing({Alerting, alert_id}) when is_integer(alert_id) do
    # sync alert job
    Utils.Tasks.start_child(fn ->
      Alerting.sync_alert_job(alert_id)
    end)
  end

  defp maybe_do_cross_cluster_syncing({Backends, backend_id})
       when is_integer(backend_id) do
    # sync backend across cluster for v2 sources
    Utils.Tasks.start_child(fn ->
      Backends.sync_backend_across_cluster(backend_id)
    end)
  end

  defp maybe_do_cross_cluster_syncing({Rules, rule_id}) when is_integer(rule_id) do
    # sync rule
    Utils.Tasks.start_child(fn ->
      Logflare.Cluster.Utils.rpc_multicall(Rules, :sync_rule, [rule_id])
    end)
  end

  defp maybe_do_cross_cluster_syncing(_), do: :noop
end
