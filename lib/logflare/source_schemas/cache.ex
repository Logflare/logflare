defmodule Logflare.SourceSchemas.Cache do
  @moduledoc false

  use Logflare.ContextCache, refresh_ahead: true

  alias Logflare.Cache.CachexOps
  alias Logflare.SourceSchemas

  def child_spec(_) do
    CachexOps.child_spec(__MODULE__,
      limit: 100_000,
      ttl: to_timeout(minute: 10),
      purge_interval: to_timeout(minute: 2),
      warmer: SourceSchemas.CacheWarmer
    )
  end

  def get_source_schema_by(kv), do: apply_fun(__ENV__.function, [kv])

  defp apply_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(SourceSchemas, arg1, arg2)
  end
end
