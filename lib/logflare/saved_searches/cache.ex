defmodule Logflare.SavedSearches.Cache do
  @moduledoc false

  use Logflare.ContextCache

  alias Logflare.Cache.CachexOps
  alias Logflare.SavedSearches

  def child_spec(_), do: CachexOps.child_spec(__MODULE__, limit: 10_000)

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
