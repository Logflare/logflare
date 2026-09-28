defmodule Logflare.ContextCache.PeerWarmer do
  @moduledoc """
  Warms a starting node's caches by copying entries from an already running peer, so that
  nodes joining the cluster don't all hydrate their caches from the database at once.

  Each supported cache publishes a status on every node: whether it is warm, when its data
  was last loaded from the database (`warmed_at`) and the format version of its entries.
  On startup, a cache warmer calls `copy_from_peer/1`, which:

    1. Waits up to `:peer_wait` for cluster peers and the local `CacheBuster`, so that no
       invalidations are missed while copying.
    2. Asks a random subset of at most `:max_peers` peers for their status and picks the one
       with the most recent `warmed_at` among the ready peers with a matching format version,
       at least one entry and, when the cache has a max age, fresh enough data.
    3. Copies entries in acknowledged chunks within `:copy_timeout`, skipping negative
       lookups, entries invalidated in the meantime (see `Logflare.ContextCache.Gossip`)
       and keys already present locally.

  When no peer qualifies or the copy fails, it returns `:fallback` and the warmer loads
  from the database as before. A copy inherits the peer's `warmed_at`, so a copy of a copy
  never looks fresher than the data it holds.
  """

  require Logger

  alias Logflare.Cluster.Utils, as: ClusterUtils
  alias Logflare.ContextCache.CacheBuster
  alias Logflare.ContextCache.Gossip
  alias Logflare.ContextCache.PeerWarmer.CachexStore
  alias Logflare.ContextCache.PeerWarmer.NebulexStore
  alias Logflare.ContextCache.PeerWarmer.Transfer
  alias Logflare.KeyValues
  alias Logflare.Sources

  @caches %{
    KeyValues.Cache => %{
      store: NebulexStore,
      target: KeyValues.Cache.L1,
      format_version: 1,
      max_age: :key_values_max_age
    },
    Sources.Cache => %{
      store: CachexStore,
      target: Sources.Cache,
      format_version: 1,
      max_age: nil
    }
  }

  @poll_interval 100

  @type state :: :warming | :ready
  @type status :: %{
          state: state(),
          warmed_at: DateTime.t() | nil,
          format_version: pos_integer(),
          size: non_neg_integer()
        }
  @type copy_result :: %{node: node(), warmed_at: DateTime.t(), count: non_neg_integer()}
  @type fallback_reason ::
          :no_peers | :no_eligible_peer | :rpc_failed | :timeout | :peer_down | :crashed

  @spec copy_from_peer(module()) :: {:ok, copy_result()} | :fallback
  def copy_from_peer(cache) when is_map_key(@caches, cache) do
    if config(:enabled) do
      :telemetry.span([:logflare, :context_cache, :peer_warm], %{cache: cache}, fn ->
        traced_copy(cache)
      end)
    else
      :fallback
    end
  end

  @spec mark_warming(module()) :: :ok
  def mark_warming(cache) when is_map_key(@caches, cache) do
    :persistent_term.put(status_key(cache), %{state: :warming, warmed_at: nil})
  end

  @spec mark_ready(module(), DateTime.t()) :: :ok
  def mark_ready(cache, %DateTime{} = warmed_at) when is_map_key(@caches, cache) do
    :persistent_term.put(status_key(cache), %{state: :ready, warmed_at: warmed_at})
  end

  @doc """
  Returns this node's status for the given cache. Called by peers over RPC.
  """
  @spec status(module()) :: status() | nil
  def status(cache) do
    with %{store: store, target: target, format_version: version} <- Map.get(@caches, cache),
         %{} = status <- :persistent_term.get(status_key(cache), nil) do
      Map.merge(status, %{format_version: version, size: store.size(target)})
    end
  end

  defp traced_copy(cache) do
    result = run_isolated(fn -> do_copy(cache) end)
    {public_result(result), %{cache: cache, outcome: outcome(result)}}
  end

  defp do_copy(cache) do
    spec = Map.fetch!(@caches, cache)
    await_ready(System.monotonic_time(:millisecond) + config(:peer_wait))

    with {:ok, node, peer_status} <- select_peer(cache, spec),
         {:ok, count} <- transfer(node, cache, spec) do
      Logger.info("Warmed #{inspect(cache)} with #{count} entries copied from #{node}")
      {:ok, %{node: node, warmed_at: peer_status.warmed_at, count: count}}
    else
      {:error, reason} = error ->
        Logger.info("Warming #{inspect(cache)} from the database: #{reason}")
        error
    end
  end

  defp run_isolated(fun) do
    task =
      Task.Supervisor.async_nolink(
        {:via, PartitionSupervisor, {Logflare.TaskSupervisors, self()}},
        fun
      )

    timeout = config(:peer_wait) + config(:copy_timeout) + 2 * Transfer.rpc_timeout()

    case Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> result
      {:exit, _reason} -> {:error, :crashed}
      nil -> {:error, :timeout}
    end
  end

  defp await_ready(deadline) do
    cond do
      peers_known?() and cache_buster_started?() -> :ok
      System.monotonic_time(:millisecond) >= deadline -> :ok
      true -> await_ready_after_sleep(deadline)
    end
  end

  defp await_ready_after_sleep(deadline) do
    Process.sleep(@poll_interval)
    await_ready(deadline)
  end

  defp peers_known? do
    Node.list() != [] or Application.get_env(:libcluster, :topologies, []) == []
  end

  defp cache_buster_started? do
    Application.get_env(:logflare, :env) == :test or Process.whereis(CacheBuster) != nil
  end

  defp select_peer(cache, spec) do
    case ClusterUtils.peer_list_partial(1.0, config(:max_peers)) do
      [] ->
        {:error, :no_peers}

      peers ->
        peers
        |> ClusterUtils.erpc_multicall(__MODULE__, :status, [cache], Transfer.rpc_timeout())
        |> Enum.filter(&eligible?(&1, spec))
        |> Enum.shuffle()
        |> Enum.max_by(fn {_node, {:ok, status}} -> status.warmed_at end, DateTime, fn -> nil end)
        |> case do
          nil -> {:error, :no_eligible_peer}
          {node, {:ok, status}} -> {:ok, node, status}
        end
    end
  end

  defp eligible?({_node, {:ok, %{state: :ready} = status}}, spec) do
    status.format_version == spec.format_version and status.size > 0 and
      fresh?(status.warmed_at, spec.max_age)
  end

  defp eligible?(_result, _spec), do: false

  defp fresh?(_warmed_at, nil), do: true

  defp fresh?(warmed_at, max_age_key) do
    DateTime.diff(DateTime.utc_now(), warmed_at, :millisecond) <= config(max_age_key)
  end

  defp transfer(node, cache, %{store: store, target: target}) do
    import_fun = fn entries ->
      importable = Enum.filter(entries, &importable?(cache, store, target, &1))
      :ok = store.put_entries(target, importable)
      length(importable)
    end

    Transfer.run(node, store, target, import_fun, config(:copy_timeout))
  end

  defp importable?(cache, store, target, entry) do
    key = store.key(entry)
    value = store.value(entry)

    value not in [nil, []] and not Gossip.stale_entry?(cache, key, value) and
      not store.exists?(target, key)
  end

  defp public_result({:ok, _copy_result} = result), do: result
  defp public_result({:error, _reason}), do: :fallback

  defp outcome({:ok, _copy_result}), do: :copied
  defp outcome({:error, reason}), do: reason

  defp status_key(cache), do: {__MODULE__, cache}

  defp config(key), do: Application.fetch_env!(:logflare, __MODULE__) |> Keyword.fetch!(key)
end
