defmodule Logflare.SavedSearches.Cache do
  @moduledoc false

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
                 Utils.cache_limit(100_000)
               ]
               |> Enum.filter(& &1),
             expiration: Utils.cache_expiration_min()
           ]
         ]}
    }
  end

  def list_saved_searches_by_user(user_id), do: apply_repo_fun(__ENV__.function, [user_id])

  def bust_by(kw) do
    kw
    |> Enum.map(fn
      {:source_id, source_id} ->
        case Logflare.Sources.get(source_id) do
          nil -> nil
          source -> {:list_saved_searches_by_user, [source.user_id]}
        end
    end)
    |> Enum.reject(&is_nil/1)
    |> Enum.reduce(0, fn key, acc ->
      case Cachex.take(__MODULE__, key) do
        {:ok, nil} -> acc
        {:ok, _value} -> acc + 1
      end
    end)
    |> then(&{:ok, &1})
  end

  defp apply_repo_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(SavedSearches, arg1, arg2)
  end
end
