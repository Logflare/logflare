defmodule Logflare.Cache.CachexOps do
  @moduledoc """
  Cachex implementations of the `Logflare.Cache` and `Logflare.ContextCache` callbacks, and builders
  for Cachex start options.

  Context cache values are stored as `{:cached, value}`, because Cachex treats a stored `nil` as a miss.
  """

  @behaviour Logflare.Cache.Ops
  @behaviour Logflare.ContextCache.Ops

  import Cachex.Spec

  alias Logflare.ContextCache.Gossip

  @stat_keys [
    :evictions,
    :expirations,
    :operations,
    :hits,
    :misses,
    :hit_rate,
    :miss_rate
  ]

  @type warmer() :: module() | {module(), interval: pos_integer()}
  @type cache_opt() ::
          {:limit, pos_integer() | nil}
          | {:ttl, pos_integer()}
          | {:purge_interval, pos_integer()}
          | {:warmer, warmer() | nil}
          | {:compressed, boolean()}

  @doc """
  Child spec for a Cachex cache registered as `name`, with the stats hook when `stats_enabled?/0`.

  Options:
    * `:limit` (required) - maximum number of entries, `nil` for no limit
    * `:ttl` - default entry time-to-live, a timeout in ms, defaults to 20 minutes
    * `:purge_interval` - timeout in ms between purges of expired entries, defaults to 5 minutes
    * `:warmer` - a non-required warmer module, or `{module, interval: ms}`, registered under the module name
    * `:compressed` - defaults to `false`
  """
  @spec child_spec(atom(), [cache_opt()]) :: Supervisor.child_spec()
  def child_spec(name, opts) do
    opts = Keyword.update!(cachex_opts(opts), :hooks, &(stats_hooks() ++ &1))
    Supervisor.child_spec({Cachex, [name, opts]}, id: name)
  end

  @doc """
  Cachex start options for `t:cache_opt/0`, without the stats hook.
  """
  @spec cachex_opts([cache_opt()]) :: keyword()
  def cachex_opts(opts) do
    [
      hooks: limit_hooks(Keyword.fetch!(opts, :limit)),
      expiration:
        expiration(
          default: Keyword.get(opts, :ttl, to_timeout(minute: 20)),
          interval: Keyword.get(opts, :purge_interval, to_timeout(minute: 5)),
          lazy: true
        ),
      warmers: warmers(opts[:warmer]),
      compressed: Keyword.get(opts, :compressed, false)
    ]
  end

  @spec stats_enabled?() :: boolean()
  def stats_enabled?, do: Application.get_env(:logflare, :cache_stats, false)

  @impl Logflare.Cache.Ops
  @spec healthy?(Cachex.t()) :: boolean()
  def healthy?(cache), do: match?({:ok, _}, Cachex.size(cache))

  @doc """
  Raises when the cache runs without the stats hook.
  """
  @impl Logflare.Cache.Ops
  @spec stats(Cachex.t()) :: Logflare.Cache.stats()
  def stats(cache) do
    {:ok, stats} = Cachex.stats(cache)

    {:total_heap_size, heap_words} =
      cache
      |> Process.whereis()
      |> Process.info(:total_heap_size)

    heap_bytes = heap_words * :erlang.system_info(:wordsize)

    for key <- @stat_keys, into: %{total_heap_size: heap_bytes} do
      {key, Map.get(stats, key, 0)}
    end
  end

  @impl Logflare.Cache.Ops
  @spec reset(Cachex.t()) :: :ok
  def reset(cache) do
    {:ok, true} = Cachex.reset(cache, hooks: [Cachex.Stats])
    :ok
  end

  @doc """
  With `[id: pkey]`, returns the keys of every entry whose cached value is a map with that `:id`, an
  `{:ok, map}` with that `:id`, or a list containing such a map. Raises `ArgumentError` for any other
  keyword.

  It scans the cache with a match spec instead of keeping a reverse index of primary keys.
  """
  @impl Logflare.ContextCache.Ops
  @spec keys_to_bust(Cachex.t(), keyword()) :: Enumerable.t()
  def keys_to_bust(cache, id: pkey) do
    filter =
      {
        # use orelse to prevent 2nd condition failing as value is not a map
        :orelse,
        {
          :orelse,
          # handle lists
          {:is_list, {:element, 2, :value}},
          # handle :ok tuples when struct with id is in 2nd element pos.
          {:andalso, {:is_tuple, {:element, 2, :value}},
           {:andalso, {:==, {:element, 1, {:element, 2, :value}}, :ok},
            {:andalso, {:is_map, {:element, 2, {:element, 2, :value}}},
             {:==, {:map_get, :id, {:element, 2, {:element, 2, :value}}}, pkey}}}}
        },
        # handle single maps
        {:andalso, {:is_map, {:element, 2, :value}},
         {:==, {:map_get, :id, {:element, 2, :value}}, pkey}}
      }

    query = Cachex.Query.build(where: filter, output: {:key, :value})

    cache
    |> Cachex.stream!(query)
    |> Stream.filter(fn
      {_k, {:cached, v}} when is_list(v) -> Enum.any?(v, &(&1.id == pkey))
      {_k, _v} -> true
    end)
    |> Stream.map(fn {k, _v} -> k end)
  end

  def keys_to_bust(cache, kw) do
    raise ArgumentError, "#{inspect(cache)} does not support busting by #{inspect(kw)}"
  end

  @doc """
  Returns the cached value for `key`, calling `getter` and caching its result on a miss.

  A miss is also multicast to peer nodes, see `Logflare.ContextCache.Gossip`. Accepts a Cachex
  worker, so calls can be batched in `Cachex.execute/2`.
  """
  @impl Logflare.ContextCache.Ops
  @spec fetch(Cachex.t(), term(), (-> term())) :: term()
  def fetch(cache, key, getter) do
    case Cachex.fetch(cache, key, fn _key -> {:commit, {:cached, getter.()}} end) do
      {:commit, {:cached, value}} ->
        Gossip.multicast(cache, key, value)
        value

      {:ok, {:cached, value}} ->
        value
    end
  end

  @impl Logflare.ContextCache.Ops
  @spec update(Cachex.t(), term(), term()) :: :ok
  def update(cache, key, value) do
    {:ok, _updated?} = Cachex.update(cache, key, {:cached, value})
    :ok
  end

  @doc """
  Deletes `keys` and returns how many of them were present.
  """
  @impl Logflare.ContextCache.Ops
  @spec delete_keys(Cachex.t(), Enumerable.t()) :: {:ok, non_neg_integer()}
  def delete_keys(cache, keys) do
    Cachex.execute(cache, fn worker ->
      Enum.reduce(keys, 0, fn key, acc -> acc + take_count(worker, key) end)
    end)
  end

  defp take_count(cache, key) do
    case Cachex.take(cache, key) do
      {:ok, nil} -> 0
      {:ok, _value} -> 1
    end
  end

  defp stats_hooks, do: if(stats_enabled?(), do: [hook(module: Cachex.Stats)], else: [])

  defp limit_hooks(nil), do: []

  defp limit_hooks(limit) when is_integer(limit) do
    [hook(module: Cachex.Limit.Scheduled, args: {limit, [], []})]
  end

  defp warmers(nil), do: []

  defp warmers({module, opts}) do
    [interval: interval] = Keyword.validate!(opts, interval: nil)
    [warmer(module: module, name: module, required: false, interval: interval)]
  end

  defp warmers(module), do: warmers({module, []})
end
