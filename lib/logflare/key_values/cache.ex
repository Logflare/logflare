defmodule Logflare.KeyValues.Cache do
  @moduledoc """
  Read-through cache for `Logflare.KeyValues` lookups and counts.

  A lookup reads this node's Cachex L1, then, when `Logflare.KeyValues.Cache.L2.enabled?/0`,
  the cluster-partitioned `Logflare.KeyValues.Cache.L2`, then the database. L2 and the
  database are read in the `Cachex.fetch/4` fallback, so concurrent misses for a key on one
  node share a single L2 call and database read. An L2 error or timeout falls back to the
  database once per lookup and skips writing the value to L2. Counts and warming stay in L1.

  A value reaches L2 only if its key-value did not change within the `:tombstone_ttl` setting.
  The key's L2 owner writes the value to its share and deletes it again if the key-value is
  tombstoned, as a lookup does for L1, so a value read from a lagging replica, `nil` included,
  cannot spread to other nodes once its change has been busted.

  ## Telemetry

    * `[:logflare, :key_values, :cache, :l2, :command, :start | :stop | :exception]` - Nebulex
      command spans of L2 on the calling node, and `[..., :l2, :primary, :command, ...]` on the
      node owning the entry
    * `[:logflare, :key_values, :cache, :l2_fill, :start | :stop | :exception]` - a value
      written to L2, with `:result` from `Logflare.KeyValues.Cache.L2.outcome/1`
    * `[:logflare, :key_values, :cache, :db_fallback]` - a lookup read from the database, with
      `:reason` `:miss` (L2 missed), `:l2_error` (L2 failed or timed out) or `:l2_skipped`
      (L2 disabled or stopped)

  L1 hits and misses are in the Cachex stats of the cache.
  """

  use Logflare.ContextCache

  alias Logflare.Cache.CachexOps
  alias Logflare.ContextCache
  alias Logflare.ContextCache.Tombstones
  alias Logflare.KeyValues
  alias Logflare.KeyValues.Cache.L2
  alias Logflare.KeyValues.CacheWarmer
  alias Logflare.Repo
  alias Nebulex.Distributed.RPC

  @db_fallback_event [:logflare, :key_values, :cache, :db_fallback]
  @l2_fill_event [:logflare, :key_values, :cache, :l2_fill]
  @supervisor Logflare.KeyValues.Cache.Supervisor
  @usage_touch_window :timer.minutes(20)
  @usage_touch_chunk 1_000

  @typep l2_result() :: {:ok, term()} | :miss | :l2_error | :l2_skipped

  @doc """
  Starts L1 and, when `Logflare.KeyValues.Cache.L2.enabled?/0`, L2 under a `one_for_one`
  supervisor, so an L2 crash does not drop the warmed L1.
  """
  def child_spec(_) do
    l1 =
      CachexOps.child_spec(__MODULE__,
        limit: 10_000_000,
        ttl: to_timeout(day: 1),
        purge_interval: to_timeout(hour: 1),
        compressed: true,
        warmer: {KeyValues.CacheWarmer, interval: :timer.hours(1)}
      )

    children = if L2.enabled?(), do: [l1, L2], else: [l1]

    %{
      id: __MODULE__,
      type: :supervisor,
      start: {Supervisor, :start_link, [children, [strategy: :one_for_one, name: @supervisor]]}
    }
  end

  @spec count(integer()) :: non_neg_integer()
  def count(user_id) do
    cache_key = {:count, user_id}

    Cachex.fetch(__MODULE__, cache_key, fn _key ->
      {:commit, {:cached, Repo.apply_with_replica(KeyValues, :count_key_values, [user_id])}}
    end)
    |> case do
      {:commit, {:cached, v}} -> v
      {:ok, {:cached, v}} -> v
    end
  end

  @spec lookup(integer(), String.t()) :: map() | nil
  def lookup(user_id, key) do
    lookup(user_id, key, nil)
  end

  @doc """
  Returns the value of a key-value, or the value at `accessor_path` within it.

  A value read before a change to the key-value, from a lagging replica, from L2 or from a
  cached full value, is returned but not kept. The change records a `{user_id, key}`
  tombstone before its bust deletes the entries, and a fill is deleted again if its key-value
  is tombstoned after the write, so no interleaving with the bust leaves a stale entry. A fill
  that starts while its key-value is tombstoned is not kept either. A tombstone lasts for the
  `:tombstone_ttl` setting, which must outlast the replica lag plus the duration of a fill.
  """
  @spec lookup(integer(), String.t(), String.t() | nil) :: term() | nil
  def lookup(user_id, key, accessor_path) do
    key_args = [user_id, key, accessor_path]
    cache_key = {:lookup, key_args}

    Cachex.execute!(__MODULE__, fn worker ->
      Cachex.fetch(worker, cache_key, fn _key -> lookup_fallback(worker, key_args) end)
    end)
    |> case do
      {:commit, {:cached, v}} ->
        drop_stale_fill(cache_key)
        v

      {:ignore, {:cached, v}} ->
        v

      {:ok, {:cached, v}} ->
        v
    end
  end

  # A bust between the fill's read and write deletes nothing, so the fill checks the tombstone
  # the bust records before deleting, once its own write is done.
  defp drop_stale_fill({:lookup, [user_id, key, _accessor_path]} = cache_key) do
    if tombstoned?(user_id, key) do
      Cachex.del(__MODULE__, cache_key)
    end
  end

  defp lookup_fallback(worker, [user_id, key, accessor_path] = key_args) do
    action = if tombstoned?(user_id, key), do: :ignore, else: :commit

    case Cachex.get(worker, {:lookup, [user_id, key, nil]}) do
      {:ok, {:cached, full_value}} ->
        {action, {:cached, KeyValues.extract_value(full_value, accessor_path)}}

      {:ok, nil} ->
        {action, {:cached, read_through(key_args)}}
    end
  end

  defp read_through(key_args) do
    cache_key = {:lookup, key_args}

    case fetch_l2(cache_key) do
      {:ok, value} ->
        value

      l2_miss ->
        {value, fill_l2?} = full_value_from_l2_or_db(l2_miss, key_args)
        if fill_l2?, do: put_l2(cache_key, value)
        value
    end
  end

  defp full_value_from_l2_or_db(:miss, [user_id, key, accessor_path] = key_args)
       when accessor_path != nil do
    case fetch_l2({:lookup, [user_id, key, nil]}) do
      {:ok, full_value} -> {KeyValues.extract_value(full_value, accessor_path), true}
      l2_miss -> {from_db(l2_miss, key_args), l2_miss == :miss}
    end
  end

  defp full_value_from_l2_or_db(l2_miss, key_args),
    do: {from_db(l2_miss, key_args), l2_miss == :miss}

  defp from_db(l2_miss, key_args) do
    :telemetry.execute(@db_fallback_event, %{count: 1}, %{reason: l2_miss})
    Repo.apply_with_replica(KeyValues, :lookup, key_args)
  end

  @spec fetch_l2(term()) :: l2_result()
  defp fetch_l2(cache_key) do
    if L2.enabled?() and l2_running?() do
      case L2.fetch(cache_key, timeout: L2.timeout()) do
        {:ok, _value} = hit -> hit
        {:error, %Nebulex.KeyError{}} -> :miss
        {:error, _reason} -> :l2_error
      end
    else
      :l2_skipped
    end
  rescue
    _ in [Nebulex.CacheNotFoundError, RuntimeError] -> :l2_error
  end

  defp put_l2(cache_key, value) do
    :telemetry.span(@l2_fill_event, %{}, fn ->
      result =
        with {:ok, owner} <- L2.find_node(cache_key) do
          RPC.call(owner, __MODULE__, :fill_l2_share, [cache_key, value, L2.ttl()], L2.timeout())
        end

      {:ok, %{result: L2.outcome(result)}}
    end)
  rescue
    _ in [Nebulex.CacheNotFoundError, RuntimeError] -> :ok
  end

  @doc false
  @spec fill_l2_share(term(), term(), timeout()) :: :put | :dropped_stale
  def fill_l2_share({:lookup, [user_id, key, _accessor_path]} = cache_key, value, ttl) do
    primary = L2.__primary__()
    :ok = primary.put(cache_key, value, ttl: ttl)

    if Tombstones.Cache.tombstoned?(__MODULE__, {user_id, key}) do
      :ok = primary.delete(cache_key)
      :dropped_stale
    else
      :put
    end
  end

  defp l2_running?, do: Process.whereis(L2) != nil

  defp tombstoned?(user_id, key), do: Tombstones.Cache.tombstoned?(__MODULE__, {user_id, key})

  @doc """
  Records which key-values were recently in use so the next full warm can
  prioritise them.

  Usage is inferred from entries that entered this node's cache since the last
  run, excluding those loaded by the warmer itself. The signal is approximate:
  it captures that a key was needed, not how often it was read.

  Returns the number of `{user_id, key}` pairs whose usage was recorded.
  """
  @spec touch_recent_usages(DateTime.t()) :: {:ok, non_neg_integer()}
  def touch_recent_usages(now \\ DateTime.utc_now()) do
    query = Cachex.Query.build(where: {:>=, :modified, touch_cutoff(now)}, output: :key)

    __MODULE__
    |> Cachex.stream!(query)
    |> Stream.flat_map(fn
      {:lookup, [user_id, key, _accessor]} -> [{user_id, key}]
      _ -> []
    end)
    |> Stream.uniq()
    |> Stream.chunk_every(@usage_touch_chunk)
    |> Enum.reduce(0, fn pairs, acc ->
      KeyValues.bump_usages(pairs, now)
      acc + length(pairs)
    end)
    |> then(&{:ok, &1})
  end

  defp touch_cutoff(now) do
    window_start = DateTime.to_unix(now, :millisecond) - @usage_touch_window

    case CacheWarmer.warmed_at() do
      nil -> window_start
      warmed_at -> max(window_start, DateTime.to_unix(warmed_at, :millisecond) + 1)
    end
  end

  @doc """
  Deletes `keys` from L1 and from this node's share of L2, without RPC, and returns the number
  of entries deleted from both.

  Every node busts every change, so the owner of an L2 entry deletes it locally. L2 entries are
  deleted while L2 runs even if it is disabled, so re-enabling it serves no entry that missed a
  bust.
  """
  @impl ContextCache
  def delete_keys(keys) do
    {:ok, l1_deleted} = CachexOps.delete_keys(__MODULE__, keys)
    {:ok, l1_deleted + Enum.count(keys, &delete_l2_local/1)}
  end

  defp delete_l2_local(cache_key) do
    l2_running?() and match?({:ok, _value}, L2.__primary__().take(cache_key))
  rescue
    Nebulex.CacheNotFoundError -> false
  end

  @impl ContextCache
  def keys_to_bust(kw) do
    user_id = Keyword.get(kw, :user_id)
    key = Keyword.get(kw, :key)

    entries = if user_id, do: [{:count, user_id}], else: []

    if user_id && key do
      lookup_keys = find_lookup_keys(user_id, key)
      lookup_keys ++ entries
    else
      entries
    end
  end

  @impl ContextCache
  def tombstones(kw) do
    with {:ok, user_id} <- Keyword.fetch(kw, :user_id),
         {:ok, key} <- Keyword.fetch(kw, :key) do
      [{{user_id, key}, tombstone_ttl()}]
    else
      :error -> []
    end
  end

  defp tombstone_ttl do
    :logflare
    |> Application.fetch_env!(__MODULE__)
    |> Keyword.fetch!(:tombstone_ttl)
  end

  defp find_lookup_keys(user_id, key) do
    {:ok, keys} = Cachex.keys(__MODULE__)

    l1_keys =
      Enum.filter(keys, fn
        {:lookup, [^user_id, ^key | _]} -> true
        _ -> false
      end)

    Enum.uniq(l1_keys ++ l2_local_lookup_keys(user_id, key))
  end

  defp l2_local_lookup_keys(user_id, key) do
    if l2_running?() do
      accessor_paths =
        L2.__primary__().get_all!(
          query: [{{:entry, {:lookup, [user_id, key, :"$1"]}, :_, :_, :_, :_}, [], [:"$1"]}]
        )

      Enum.map(accessor_paths, &{:lookup, [user_id, key, &1]})
    else
      []
    end
  rescue
    Nebulex.CacheNotFoundError -> []
  end
end
