defmodule Logflare.KeyValues.Cache do
  @moduledoc """
  Read-through cache for `Logflare.KeyValues` lookups and counts.

  A lookup is served by the first of two levels that holds it, then by the database:

    * L1 - a Cachex cache on each node, warmed with recently used key-values by
      `Logflare.KeyValues.CacheWarmer`; counts are cached here only
    * L2 - `Logflare.KeyValues.Cache.L2`, partitioned across the cluster, so a key-value read
      from the database on one node is served to the others. It is off unless
      `Logflare.KeyValues.Cache.L2.enabled?/0`, and a lookup falls back to the database while
      it is unavailable

  A change to a key-value is busted from both levels on every node. It also tombstones the
  key-value for the `:tombstone_ttl` setting, during which values read for it are returned but
  kept in neither level, so a value read before the change, e.g. from a lagging replica, is
  not cached after the bust.

  ## Telemetry

    * `[:cachex, :key_values]` - L1 stats (hits, misses, evictions, ...), polled by
      `Logflare.Telemetry` when the `:cache_stats` setting is on
    * `[:logflare, :key_values, :cache, :l2, ...]` - Nebulex events of L2: `:command` spans on
      the calling node, `[:primary, :command]` spans on the node owning the entry and
      `:ring_monitor` events when nodes join or leave the hash ring
    * `[:logflare, :key_values, :cache, :l2, :primary]` - size and memory of this node's share
      of L2, polled by `Logflare.Telemetry`
    * `[:logflare, :key_values, :cache, :l2_fill, :start | :stop | :exception]` - a value
      written to L2, with `:result` `:put`, `:dropped_stale` or `:error`
    * `[:logflare, :key_values, :cache, :db_fallback]` - a lookup read from the database, with
      `:reason` `:miss` (L2 missed), `:l2_error` (L2 failed or timed out) or `:l2_skipped`
      (L2 disabled)
  """

  use Logflare.ContextCache

  alias Logflare.Cache.CachexOps
  alias Logflare.ContextCache
  alias Logflare.ContextCache.Tombstones
  alias Logflare.KeyValues
  alias Logflare.KeyValues.Cache.L2
  alias Logflare.KeyValues.CacheWarmer
  alias Logflare.Repo

  @db_fallback_event [:logflare, :key_values, :cache, :db_fallback]
  @supervisor Logflare.KeyValues.Cache.Supervisor
  @usage_touch_window :timer.minutes(20)
  @usage_touch_chunk 1_000

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

  defp read_through([user_id, key, accessor_path] = key_args) do
    full_key = {:lookup, [user_id, key, nil]}

    case L2.lookup(full_key) do
      {:ok, full_value} ->
        extract_value(full_value, accessor_path)

      :miss ->
        full_value = from_db(:miss, [user_id, key, nil])
        L2.fill(full_key, full_value)
        extract_value(full_value, accessor_path)

      l2_unavailable ->
        from_db(l2_unavailable, key_args)
    end
  end

  defp extract_value(full_value, nil), do: full_value

  defp extract_value(full_value, accessor_path),
    do: KeyValues.extract_value(full_value, accessor_path)

  defp from_db(l2_miss, key_args) do
    :telemetry.execute(@db_fallback_event, %{count: 1}, %{reason: l2_miss})
    Repo.apply_with_replica(KeyValues, :lookup, key_args)
  end

  defp tombstoned?(user_id, key), do: Tombstones.Cache.tombstoned?(__MODULE__, {user_id, key})

  @doc """
  Clears this node's L1 and its Cachex stats, and L2 on every node.
  """
  @impl Logflare.Cache
  def reset do
    :ok = CachexOps.reset(__MODULE__)
    _ = L2.clear()
    :ok
  end

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
  Deletes `keys` from L1, then from L2 in one call, and returns the number of entries deleted
  from both levels.
  """
  @impl ContextCache
  def delete_keys(keys) do
    with {:ok, from_l1} <-
           Cachex.execute(__MODULE__, fn worker ->
             Enum.count(keys, &(not match?({:ok, nil}, Cachex.take(worker, &1))))
           end) do
      {:ok, from_l1 + L2.evict(Enum.filter(keys, &match?({:lookup, [_user_id, _key, nil]}, &1)))}
    end
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

    Enum.uniq([{:lookup, [user_id, key, nil]} | l1_keys])
  end
end
