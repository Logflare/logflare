defmodule Logflare.Cache do
  @moduledoc """
  Operational contract of an application cache, independent of its storage backend.

  `use Logflare.Cache` injects default implementations of every callback, delegating to
  `Logflare.Cache.CachexOps`. A cache on another backend overrides them, or passes
  `impl: module` whose `healthy?/1`, `stats/1` and `reset/1` take the cache module.
  """

  alias Logflare.Cache.CachexOps

  @typedoc """
  Counters since the last `c:reset/0`. Rates are percentages (0-100); `total_heap_size` is in words.
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

  defmacro __using__(opts) do
    impl = Keyword.get(opts, :impl, CachexOps)

    quote do
      @behaviour Logflare.Cache

      @impl Logflare.Cache
      def healthy?, do: unquote(impl).healthy?(__MODULE__)

      @impl Logflare.Cache
      def stats, do: unquote(impl).stats(__MODULE__)

      @impl Logflare.Cache
      def reset, do: unquote(impl).reset(__MODULE__)

      defoverridable healthy?: 0, stats: 0, reset: 0
    end
  end
end
