defmodule Logflare.Cache do
  @moduledoc """
  Operational contract of an application cache, independent of its storage backend.

  Caches on Cachex implement the callbacks by delegating to `Logflare.Cache.CachexOps`.
  """

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
end
