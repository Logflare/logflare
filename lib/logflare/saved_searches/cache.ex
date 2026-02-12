defmodule Logflare.SavedSearches.Cache do
  @moduledoc false

  @behaviour Logflare.ContextCache

  alias Logflare.SavedSearches
  alias Logflare.Utils

  def child_spec(_) do
    %{
      id: __MODULE__,
      start:
        {Cachex, :start_link,
         [
           __MODULE__,
           [
             hooks:
               [
                 Utils.cache_stats(),
                 Utils.cache_limit(10_000)
               ]
               |> Enum.filter(& &1),
             expiration: Utils.cache_expiration_min()
           ]
         ]}
    }
  end

  def list_saved_searches_by_source(source_id), do: apply_repo_fun(__ENV__.function, [source_id])

  @impl Logflare.ContextCache
  def bust_by(kw) do
    entries =
      kw
      |> Enum.map(fn
        {:source_id, source_id} -> {:list_saved_searches_by_source, [source_id]}
      end)

    Cachex.execute(__MODULE__, fn cache ->
      Enum.reduce(entries, 0, fn key, acc ->
        acc + delete_and_count(cache, key)
      end)
    end)
  end

  defp delete_and_count(cache, key) do
    case Cachex.take(cache, key) do
      {:ok, nil} -> 0
      {:ok, _value} -> 1
    end
  end

  defp apply_repo_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(SavedSearches, arg1, arg2)
  end
end
