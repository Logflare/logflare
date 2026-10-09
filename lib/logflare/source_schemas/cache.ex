defmodule Logflare.SourceSchemas.Cache do
  @moduledoc false

  @behaviour Logflare.Cache
  @behaviour Logflare.ContextCache

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

  @impl Logflare.Cache
  def healthy?, do: CachexOps.healthy?(__MODULE__)

  @impl Logflare.Cache
  def stats, do: CachexOps.stats(__MODULE__)

  @impl Logflare.Cache
  def reset, do: CachexOps.reset(__MODULE__)

  @impl Logflare.ContextCache
  def fetch(key, getter), do: CachexOps.fetch(__MODULE__, key, getter)

  @impl Logflare.ContextCache
  def update(key, value), do: CachexOps.update(__MODULE__, key, value)

  @impl Logflare.ContextCache
  def keys_to_bust(kw), do: CachexOps.keys_to_bust(__MODULE__, kw)

  @impl Logflare.ContextCache
  def delete_keys(keys), do: CachexOps.delete_keys(__MODULE__, keys)

  def get_source_schema_by(kv), do: apply_fun(__ENV__.function, [kv])

  defp apply_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(SourceSchemas, arg1, arg2)
  end
end
