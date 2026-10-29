defmodule Logflare.SourceSchemas.Cache do
  @moduledoc false

  use Logflare.ContextCache

  alias Logflare.Cache.CachexOps
  alias Logflare.ContextCache.Warmer
  alias Logflare.SourceSchemas

  def child_spec(_) do
    ttl = to_timeout(minute: 10)

    CachexOps.child_spec(__MODULE__,
      limit: 100_000,
      ttl: ttl,
      purge_interval: to_timeout(minute: 2),
      warmer: {SourceSchemas.CacheWarmer, interval: Warmer.interval(ttl)}
    )
  end

  def get_source_schema_by(kv), do: apply_fun(__ENV__.function, [kv])

  defp apply_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(SourceSchemas, arg1, arg2)
  end
end
