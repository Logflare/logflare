defmodule Logflare.ContextCache.CacheBusterWorker do
  @moduledoc """
    Monitors our Postgres replication log and busts the cache accordingly.
  """

  use GenServer

  require Logger

  alias Logflare.Backends
  alias Logflare.ContextCache
  alias Logflare.Repo
  alias Logflare.Rules
  alias Logflare.Utils

  @supervisor_name __MODULE__.Supervisor

  @spec supervisor_spec() :: Supervisor.module_spec()
  def supervisor_spec do
    {PartitionSupervisor, child_spec: __MODULE__, name: @supervisor_name}
  end

  @spec cast_to_bust([{context, args}], [pos_integer()]) :: :ok
        when context: module(), args: term()
  def cast_to_bust(records, backend_owner_ids \\ []) do
    GenServer.cast(
      {:via, PartitionSupervisor, {@supervisor_name, records}},
      {:to_bust, records, backend_owner_ids}
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
  def handle_cast({:to_bust, context_pkeys, backend_owner_ids}, state) do
    ContextCache.Gossip.record_tombstones(context_pkeys)
    backend_ids = backend_ids(context_pkeys)

    default_ingest_user_ids =
      backend_ids
      |> Backends.cached_default_ingest_user_ids()
      |> Enum.concat(backend_owner_ids)
      |> Enum.uniq()

    ContextCache.bust_keys(context_pkeys)
    refresh_default_ingest_caches(default_ingest_user_ids)
    bust_rules_trees(backend_ids)

    for record <- context_pkeys do
      maybe_do_cross_cluster_syncing(record)
    end

    {:noreply, state}
  end

  @impl true
  def handle_info({:bust_rules_trees, backend_ids}, state) do
    bust_rules_trees(backend_ids)
    {:noreply, state}
  end

  defp refresh_default_ingest_caches([]), do: :ok

  defp refresh_default_ingest_caches(user_ids) do
    Utils.Tasks.start_child(fn -> Enum.each(user_ids, &refresh_default_ingest_cache/1) end)

    :ok
  end

  defp refresh_default_ingest_cache(user_id) do
    Backends.refresh_default_ingest_backends_cache(user_id)
  catch
    :exit, reason ->
      Logger.warning("Default ingest lookup refresh exited: #{inspect(reason)}",
        user_id: user_id
      )
  end

  defp backend_ids(context_pkeys) do
    for {Backends, backend_id} when is_integer(backend_id) <- context_pkeys, do: backend_id
  end

  @spec bust_rules_trees([pos_integer()]) :: :ok
  defp bust_rules_trees([]), do: :ok

  defp bust_rules_trees(backend_ids) do
    case Repo.with_primary(fn -> Rules.list_source_ids_by_backend_ids(backend_ids) end) do
      [] ->
        :ok

      source_ids ->
        source_ids
        |> Enum.map(&{Rules, [source_id: &1]})
        |> ContextCache.bust_keys()

        :ok
    end
  rescue
    error in [DBConnection.ConnectionError, Postgrex.Error] ->
      retry_rules_tree_bust(backend_ids, error)
  catch
    :exit, {reason, _call} when reason in [:noproc, :killed] ->
      retry_rules_tree_bust(backend_ids, reason)
  end

  @spec retry_rules_tree_bust([pos_integer()], term()) :: :ok
  defp retry_rules_tree_bust(backend_ids, reason) do
    Logger.warning("Rule cache invalidation failed: #{inspect(reason)}")
    Process.send_after(self(), {:bust_rules_trees, backend_ids}, to_timeout(second: 1))
    :ok
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
