defmodule Logflare.Cache.CachexOps do
  @moduledoc """
  Cachex implementations of the `Logflare.Cache` callbacks, and builders for Cachex start options.
  """

  import Cachex.Spec

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

  @spec healthy?(Cachex.t()) :: boolean()
  def healthy?(cache), do: match?({:ok, _}, Cachex.size(cache))

  @doc """
  Raises when the cache runs without the stats hook.
  """
  @spec stats(Cachex.t()) :: Logflare.Cache.stats()
  def stats(cache) do
    {:ok, stats} = Cachex.stats(cache)

    {:total_heap_size, total_heap_size} =
      cache
      |> Process.whereis()
      |> Process.info(:total_heap_size)

    for key <- @stat_keys, into: %{total_heap_size: total_heap_size} do
      {key, Map.get(stats, key, 0)}
    end
  end

  @spec reset(Cachex.t()) :: :ok
  def reset(cache) do
    {:ok, true} = Cachex.reset(cache, hooks: [Cachex.Stats])
    :ok
  end

  @doc """
  Deletes `keys` and returns how many of them were present.
  """
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
