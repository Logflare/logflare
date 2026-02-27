defmodule Logflare.ContextCache.CacheBusterWorker do
  @moduledoc """
  Executes cache busting and in-place updates via a PartitionSupervisor for parallelism.
  ETS scans with matchspecs (especially for list entries) can be expensive, so this work
  is offloaded from the CacheBuster GenServer to avoid blocking PubSub message processing.
  """

  use GenServer

  require Logger

  alias Logflare.Backends
  alias Logflare.ContextCache
  alias Logflare.Rules
  alias Logflare.Utils

  @supervisor_name __MODULE__.Supervisor

  @spec supervisor_spec() :: Supervisor.module_spec()
  def supervisor_spec() do
    {PartitionSupervisor, child_spec: __MODULE__, name: @supervisor_name}
  end

  @spec cast_apply([{:fetched, {module(), integer(), struct()}} | {:bust, {module(), term()}}]) ::
          :ok
  def cast_apply(results) when is_list(results) do
    GenServer.cast(
      {:via, PartitionSupervisor, {@supervisor_name, results}},
      {:apply, results}
    )
  end

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args)
  end

  @impl true
  def init(state) do
    {:ok, state}
  end

  @impl true
  def handle_cast({:apply, results}, state) do
    grouped = Enum.group_by(results, &elem(&1, 0), &elem(&1, 1))

    ContextCache.refresh_keys(Map.get(grouped, :fetched, []))
    ContextCache.bust_keys(Map.get(grouped, :bust, []))

    for {_tag, value} <- results do
      maybe_do_cross_cluster_syncing(value)
    end

    {:noreply, state}
  end


  defp maybe_do_cross_cluster_syncing({Backends, backend_id})
       when is_integer(backend_id) do
    Utils.Tasks.start_child(fn ->
      Backends.sync_backend_across_cluster(backend_id)
    end)
  end

  defp maybe_do_cross_cluster_syncing({Rules, rule_id}) when is_integer(rule_id) do
    Utils.Tasks.start_child(fn ->
      Rules.sync_rule(rule_id)
    end)
  end

  defp maybe_do_cross_cluster_syncing({context, pkey, _struct}),
    do: maybe_do_cross_cluster_syncing({context, pkey})

  defp maybe_do_cross_cluster_syncing(_), do: :noop
end
