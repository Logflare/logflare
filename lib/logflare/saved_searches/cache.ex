defmodule Logflare.SavedSearches.Cache do
  @moduledoc false

  @behaviour Logflare.ContextCache

  alias Logflare.SavedSearches
  alias Logflare.Utils

  def child_spec(_) do
    stats = Application.get_env(:logflare, :cache_stats, false)

    %{
      id: __MODULE__,
      start:
        {Cachex, :start_link,
         [
           __MODULE__,
           [
             hooks:
               [
                 if(stats, do: Utils.cache_stats()),
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
        case Cachex.take(cache, key) do
          {:ok, nil} -> acc
          {:ok, _value} -> acc + 1
        end
      end)
    end)
  end

  defp apply_repo_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(SavedSearches, arg1, arg2)
  end
end
