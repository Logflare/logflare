defmodule Logflare.SavedSearches.Cache do
  @moduledoc false

  use Logflare.ContextCache

  alias Logflare.Cache.CachexOps
  alias Logflare.Repo
  alias Logflare.SavedSearch
  alias Logflare.SavedSearches

  def child_spec(_), do: CachexOps.child_spec(__MODULE__, limit: 10_000)

  @spec list_by_sources([pos_integer()]) :: [SavedSearch.t()]
  def list_by_sources(source_ids) do
    source_ids = source_ids |> Enum.uniq() |> Enum.sort()

    Cachex.execute!(__MODULE__, &list_by_sources(&1, source_ids))
  end

  @impl Logflare.ContextCache
  def keys_to_bust(kw) do
    Enum.map(kw, fn
      {:source_id, source_id} -> {:list_by_sources, [source_id]}
    end)
  end

  @spec list_by_sources(Cachex.t(), [pos_integer()]) :: [SavedSearch.t()]
  defp list_by_sources(cache, source_ids) do
    entries =
      Enum.map(source_ids, fn source_id ->
        {source_id, Cachex.get!(cache, {:list_by_sources, [source_id]})}
      end)

    missing_ids = for {source_id, nil} <- entries, do: source_id

    fetched =
      Repo.apply_with_replica(SavedSearches, :list_by_sources, [missing_ids])
      |> Enum.group_by(& &1.source_id)

    Enum.flat_map(entries, fn
      {_source_id, {:cached, searches}} ->
        searches

      {source_id, nil} ->
        CachexOps.fetch(cache, {:list_by_sources, [source_id]}, fn ->
          Map.get(fetched, source_id, [])
        end)
    end)
  end
end
