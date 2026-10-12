defmodule Logflare.Cache do
  @moduledoc """
  Operational contract of an application cache, independent of its storage backend.

  Every callback is optional. Generic code calls the functions of this module, which fall back to
  `Logflare.Cache.CachexOps` for callbacks the cache does not implement.
  """

  alias Logflare.Cache.CachexOps

  @typedoc """
  Counters since the last `c:reset/0`. Rates are percentages (0-100); `total_heap_size` is in bytes.
  """
  @type stats() :: %{
          evictions: non_neg_integer(),
          expirations: non_neg_integer(),
          operations: non_neg_integer(),
          hits: non_neg_integer(),
          misses: non_neg_integer(),
          hit_rate: number(),
          miss_rate: number(),
          total_heap_size: non_neg_integer()
        }

  @doc "Whether the cache on this node can serve requests."
  @callback healthy?() :: boolean()

  @callback stats() :: stats()

  @doc "Clears all entries and statistics."
  @callback reset() :: :ok

  @optional_callbacks healthy?: 0, stats: 0, reset: 0

  @spec healthy?(module()) :: boolean()
  def healthy?(cache), do: dispatch(cache, :healthy?, [])

  @spec stats(module()) :: stats()
  def stats(cache), do: dispatch(cache, :stats, [])

  @spec reset(module()) :: :ok
  def reset(cache), do: dispatch(cache, :reset, [])

  @doc """
  Calls `fun` on `cache` when the cache implements it, otherwise the `Logflare.Cache.CachexOps`
  function of the same name with `cache` prepended to `args`.
  """
  @spec dispatch(module(), atom(), list()) :: term()
  def dispatch(cache, fun, args) do
    if Code.ensure_loaded?(cache) and function_exported?(cache, fun, length(args)),
      do: apply(cache, fun, args),
      else: apply(CachexOps, fun, [cache | args])
  end
end
