defmodule Logflare.KeyValues.Cache.L2 do
  @moduledoc """
  Distributed (L2) level of `Logflare.KeyValues.Cache`, consulted on an L1 miss before the
  database. It holds only full values, under the `{:lookup, [user_id, key, nil]}` keys of L1;
  accessor lookups extract from them.

  Keys are partitioned across the cluster by a consistent hash ring; each node holds its
  share in a `Nebulex.Adapters.Local` primary storage, reachable through `__primary__/0`
  without RPC. It is off unless `enabled?/0`, and the settings below live under
  `config :logflare, Logflare.KeyValues.Cache`:

    * `:l2_enabled` - starts the level at boot; the cache treats an enabled level as running,
      so it is not meant to change at runtime
    * `:l2_timeout` - timeout in ms of an L2 call; a slower call counts as an error
    * `:l2_ttl` - time-to-live in ms of an entry written to L2

  Adapter options come from `config :logflare, Logflare.KeyValues.Cache.L2`. The defaults:

    * `primary: [compressed: true]` - values are JSON maps, compressed as in L1
    * `primary: [gc_interval: 1h]` - generations rotate hourly, so with the 1h `:l2_ttl`
      expired entries are reclaimed within two hours even if never read again
    * `primary: [max_size: 2_000_000, allocated_memory: 1 GiB]` - caps one node's share;
      crossing either drops the older generation instead of growing the VM
    * `rejoin_interval: 30s` - the adapter default; a missed `:pg` join only routes keys to
      a different owner (a miss), never serves a wrong value, so faster convergence buys little
    * `hash_ring` - `ExHashRing` defaults (512 vnodes per node)

  Lookups and busts run each L2 call in a separate task bounded by `:l2_timeout`, so a failing
  or restarting L2 counts as unavailable instead of crashing the caller.
  """

  use Nebulex.Cache,
    otp_app: :logflare,
    adapter: Nebulex.Adapters.Partitioned

  alias Logflare.ContextCache.Tombstones
  alias Logflare.KeyValues
  alias Logflare.Utils.Tasks

  @fill_event [:logflare, :key_values, :cache, :l2_fill]

  @type result() :: {:ok, term()} | :miss | :l2_error | :l2_skipped

  @spec enabled?() :: boolean()
  def enabled?, do: Keyword.get(settings(), :l2_enabled, false)

  @spec timeout() :: timeout()
  def timeout, do: Keyword.get(settings(), :l2_timeout, 50)

  @spec ttl() :: timeout()
  def ttl, do: Keyword.get(settings(), :l2_ttl, to_timeout(hour: 1))

  @doc """
  Fetches a value from its owner's share, or returns why it could not.

  `:l2_error` covers an error, crash or timeout of the call, and `:l2_skipped` a disabled level.
  """
  @spec lookup(term()) :: result()
  def lookup(cache_key) do
    if enabled?(), do: fetch_isolated(cache_key), else: :l2_skipped
  end

  defp fetch_isolated(cache_key) do
    fetch = fn -> __MODULE__.fetch(cache_key, timeout: timeout()) end

    case Tasks.run_within(fetch, timeout()) do
      {:ok, {:ok, _value} = hit} -> hit
      {:ok, {:error, %Nebulex.KeyError{}}} -> :miss
      _failed -> :l2_error
    end
  end

  @doc """
  Writes a full value to its owner's share in a separate task, and deletes it again if its
  key-value is tombstoned on this node once the write is done.

  Every node records the tombstone of a change before busting it from L2, so either the check
  sees the tombstone or this node's bust deletes the entry after the write.
  """
  @spec fill(term(), term()) :: :ok
  def fill(cache_key, value) do
    _ = Tasks.start_child(fn -> put_unless_tombstoned(cache_key, value) end)
    :ok
  end

  defp put_unless_tombstoned({:lookup, [user_id, key, nil]} = cache_key, value) do
    :telemetry.span(@fill_event, %{}, fn ->
      result =
        with :ok <- __MODULE__.put(cache_key, value, ttl: ttl(), timeout: timeout()) do
          drop_if_tombstoned(cache_key, user_id, key)
        else
          {:error, _reason} -> :error
        end

      {:ok, %{result: result}}
    end)
  end

  defp drop_if_tombstoned(cache_key, user_id, key) do
    with true <- Tombstones.Cache.tombstoned?(KeyValues.Cache, {user_id, key}),
         :ok <- __MODULE__.delete(cache_key, timeout: timeout()) do
      :dropped_stale
    else
      false -> :put
      {:error, _reason} -> :error
    end
  end

  @doc """
  Deletes keys from their owners' shares, one call per owner node, and returns the number of
  entries deleted.
  """
  @spec evict([term()]) :: non_neg_integer()
  def evict(cache_keys) do
    delete = fn -> __MODULE__.delete_all([in: cache_keys], timeout: timeout()) end

    with true <- enabled?(),
         {:ok, {:ok, count}} <- Tasks.run_within(delete, timeout()) do
      count
    else
      _disabled_or_failed -> 0
    end
  end

  @doc """
  Deletes every entry on every node.
  """
  @spec clear() :: :ok
  def clear do
    if enabled?(), do: {:ok, _count} = __MODULE__.delete_all()
    :ok
  end

  defp settings, do: Application.get_env(:logflare, Logflare.KeyValues.Cache, [])
end
