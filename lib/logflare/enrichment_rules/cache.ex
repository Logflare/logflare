defmodule Logflare.EnrichmentRules.Cache do
  @moduledoc false

  use Logflare.ContextCache

  alias Logflare.Cache.CachexOps
  alias Logflare.ContextCache
  alias Logflare.EnrichmentRules

  def child_spec(_) do
    CachexOps.child_spec(__MODULE__,
      limit: 100_000,
      ttl: to_timeout(hour: 1),
      purge_interval: to_timeout(minute: 5)
    )
  end

  @spec compiled_by_source_id(integer()) :: EnrichmentRules.compiled() | nil
  def compiled_by_source_id(source_id),
    do: ContextCache.apply_fun(EnrichmentRules, __ENV__.function, [source_id])

  @impl ContextCache
  def keys_to_bust(kw) do
    Enum.flat_map(kw, fn
      {:source_id, source_id} -> [{:compiled_by_source_id, [source_id]}]
      _ -> []
    end)
  end
end
