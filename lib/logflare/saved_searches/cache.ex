defmodule Logflare.SavedSearches.Cache do
  @moduledoc false

  @behaviour Logflare.Cache
  @behaviour Logflare.ContextCache

  alias Logflare.Cache.CachexOps
  alias Logflare.SavedSearches

  def child_spec(_), do: CachexOps.child_spec(__MODULE__, limit: 10_000)

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
  def delete_keys(keys), do: CachexOps.delete_keys(__MODULE__, keys)

  def list_saved_searches_by_source(source_id), do: apply_repo_fun(__ENV__.function, [source_id])

  @impl Logflare.ContextCache
  def keys_to_bust(kw) do
    Enum.map(kw, fn
      {:source_id, source_id} -> {:list_saved_searches_by_source, [source_id]}
    end)
  end

  defp apply_repo_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(SavedSearches, arg1, arg2)
  end
end
