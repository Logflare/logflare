defmodule Logflare.ContextCache.PeerWarmer do
  @moduledoc """
  Warms a starting node's context caches by copying entries from an already running peer, so
  that nodes joining the cluster don't all hydrate their caches from the database at once.

  Each supported cache publishes a status on every node: whether it is warm and when its data
  was last loaded from the database (`warmed_at`). On startup, a cache warmer calls
  `copy_from_peer/1`, which:

    1. Waits up to `:peer_wait` for cluster peers and the local `CacheBuster`. Without a running
       `CacheBuster`, invalidations during the copy would be missed, so it does not copy.
    2. Asks up to `:max_peers` random peers, one after another, to send the cache. Each peer
       decides itself with `source_status/2` whether it may serve the copy: it must be ready, run
       the same release, hold at least one entry and, when the cache has a max age, have fresh
       enough data. Picking peers randomly spreads the copy load.
    3. Copies `c:Logflare.ContextCache.entries/0` in acknowledged chunks, skipping negative
       lookups, entries for which `c:Logflare.ContextCache.stale_entry?/2` holds and keys already
       cached locally. All attempts share one `:copy_timeout`.

  When every peer refuses or fails, it returns `:fallback` and the warmer loads from the
  database as before. A copy inherits the peer's `warmed_at`, so a copy of a copy never looks
  fresher than the data it holds.
  """

  require Logger

  alias Logflare.Cluster.Utils, as: ClusterUtils
  alias Logflare.ContextCache.CacheBuster
  alias Logflare.ContextCache.PeerWarmer.Transfer
  alias Logflare.KeyValues
  alias Logflare.Sources

  @max_age_config %{
    KeyValues.Cache => :key_values_max_age,
    Sources.Cache => nil
  }

  @poll_interval 100

  @type state :: :warming | :ready
  @type status :: %{
          state: state(),
          warmed_at: DateTime.t() | nil,
          version: charlist() | nil,
          size: non_neg_integer()
        }
  @type copy_result :: %{node: node(), warmed_at: DateTime.t(), count: non_neg_integer()}
  @type fallback_reason ::
          :cache_buster_not_started
          | :no_peers
          | :no_eligible_peer
          | :rpc_failed
          | :timeout
          | :peer_down
          | :crashed

  @spec copy_from_peer(module()) :: {:ok, copy_result()} | :fallback
  def copy_from_peer(cache) when is_map_key(@max_age_config, cache) do
    if config(:enabled) do
      :telemetry.span([:logflare, :context_cache, :peer_warm], %{cache: cache}, fn ->
        traced_copy(cache)
      end)
    else
      :fallback
    end
  end

  @spec mark_warming(module()) :: :ok
  def mark_warming(cache) when is_map_key(@max_age_config, cache) do
    :persistent_term.put(status_key(cache), %{state: :warming, warmed_at: nil})
  end

  @spec mark_ready(module(), DateTime.t()) :: :ok
  def mark_ready(cache, %DateTime{} = warmed_at) when is_map_key(@max_age_config, cache) do
    :persistent_term.put(status_key(cache), %{state: :ready, warmed_at: warmed_at})
  end

  @doc """
  Returns this node's status for the given cache.
  """
  @spec status(module()) :: status() | nil
  def status(cache) when is_map_key(@max_age_config, cache) do
    with %{} = status <- :persistent_term.get(status_key(cache), nil) do
      Map.merge(status, %{version: release_version(), size: cache.size()})
    end
  end

  def status(_cache), do: nil

  @doc """
  Whether this node may send `cache` to a node running release `version`. Called on the peer
  right before it starts sending, so the check and the copy see the same state.
  """
  @spec source_status(module(), charlist() | nil) ::
          {:ok, %{warmed_at: DateTime.t()}} | {:error, :not_eligible}
  def source_status(cache, version) do
    case status(cache) do
      %{state: :ready, version: ^version, size: size, warmed_at: warmed_at} when size > 0 ->
        if fresh?(warmed_at, Map.fetch!(@max_age_config, cache)),
          do: {:ok, %{warmed_at: warmed_at}},
          else: {:error, :not_eligible}

      _not_eligible ->
        {:error, :not_eligible}
    end
  end

  defp traced_copy(cache) do
    result = run_isolated(fn -> do_copy(cache) end)
    {public_result(result), %{cache: cache, outcome: outcome(result)}}
  end

  defp do_copy(cache) do
    await_ready(System.monotonic_time(:millisecond) + config(:peer_wait))

    with :ok <- ensure_cache_buster_started(),
         {:ok, peers} <- pick_peers(),
         {:ok, node, count, warmed_at} <- copy_from_first_available(peers, cache) do
      Logger.info("Warmed #{inspect(cache)} with #{count} entries copied from #{node}")
      {:ok, %{node: node, warmed_at: warmed_at, count: count}}
    else
      {:error, reason} = error ->
        Logger.info(
          "Could not copy #{inspect(cache)} from a peer node (reason: #{reason}), " <>
            "warming it from the database instead"
        )

        error
    end
  end

  defp run_isolated(fun) do
    task =
      Task.Supervisor.async_nolink(
        {:via, PartitionSupervisor, {Logflare.TaskSupervisors, self()}},
        fun
      )

    timeout =
      config(:peer_wait) + config(:copy_timeout) + config(:max_peers) * Transfer.rpc_timeout()

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
    not config(:await_cache_buster) or Process.whereis(CacheBuster) != nil
  end

  defp ensure_cache_buster_started do
    if cache_buster_started?(), do: :ok, else: {:error, :cache_buster_not_started}
  end

  defp pick_peers do
    case ClusterUtils.peer_list_partial(1.0, config(:max_peers)) do
      [] -> {:error, :no_peers}
      peers -> {:ok, peers}
    end
  end

  defp copy_from_first_available(peers, cache) do
    deadline = System.monotonic_time(:millisecond) + config(:copy_timeout)
    guard = {__MODULE__, :source_status, [cache, release_version()]}

    Enum.reduce_while(peers, {:error, :no_eligible_peer}, fn node, last_error ->
      case deadline - System.monotonic_time(:millisecond) do
        remaining when remaining > 0 ->
          node
          |> Transfer.run(cache, guard, &import_entries(cache, &1), remaining)
          |> attempt_result(node, last_error)

        _deadline_passed ->
          {:halt, {:error, :timeout}}
      end
    end)
  end

  defp attempt_result({:ok, count, %{warmed_at: warmed_at}}, node, _last_error),
    do: {:halt, {:ok, node, count, warmed_at}}

  defp attempt_result({:error, :not_eligible}, _node, last_error), do: {:cont, last_error}
  defp attempt_result({:error, _reason} = error, _node, _last_error), do: {:cont, error}

  defp fresh?(_warmed_at, nil), do: true

  defp fresh?(warmed_at, max_age_key) do
    DateTime.diff(DateTime.utc_now(), warmed_at, :millisecond) <= config(max_age_key)
  end

  defp import_entries(cache, entries) do
    importable = Enum.filter(entries, &importable?(cache, &1))
    :ok = cache.put_entries(importable)
    length(importable)
  end

  defp importable?(cache, {key, value, _ttl}) do
    value not in [nil, []] and not cache.stale_entry?(key, value) and not cache.cached?(key)
  end

  defp public_result({:ok, _copy_result} = result), do: result
  defp public_result({:error, _reason}), do: :fallback

  defp outcome({:ok, _copy_result}), do: :copied
  defp outcome({:error, reason}), do: reason

  defp release_version, do: Application.spec(:logflare, :vsn)

  defp status_key(cache), do: {__MODULE__, cache}

  defp config(key), do: Application.fetch_env!(:logflare, __MODULE__) |> Keyword.fetch!(key)
end
