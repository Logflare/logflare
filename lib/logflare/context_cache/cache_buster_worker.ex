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

  @supervisor_name __MODULE__.Supervisor

  @spec supervisor_spec() :: Supervisor.module_spec()
  def supervisor_spec() do
    {PartitionSupervisor, child_spec: __MODULE__, name: @supervisor_name}
  end

  @spec cast_to_bust([{action, {context, args}}]) :: :ok
        when action: :bust | :refresh, context: module(), args: term()
  def cast_to_bust(records) do
    GenServer.cast({:via, PartitionSupervisor, {@supervisor_name, records}}, {:to_bust, records})
  end

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args)
  end

  @impl true
  def init(state) do
    {:ok, state}
  end

  @impl true
  def handle_cast({:to_bust, records}, state) do
    grouped = Enum.group_by(records, &elem(&1, 0), &elem(&1, 1))

    ContextCache.bust_keys(Map.get(grouped, :bust, []))
    ContextCache.refresh_keys(Map.get(grouped, :refresh, []))

    for {_action, ctx_key} <- records do
      maybe_do_cross_cluster_syncing(ctx_key)
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
      Rules.sync_rule(rule_id)
    end)
  end

  defp maybe_do_cross_cluster_syncing(_), do: :noop
end
