defmodule Logflare.Endpoints.Cache do
  @moduledoc """
  Cachex for Endpoints context.
  """

  @behaviour Logflare.ContextCache

  alias Logflare.ContextCache
  alias Logflare.Endpoints
  alias Logflare.Repo
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
               |> Enum.reject(&is_nil/1),
             expiration: Utils.cache_expiration_min(2, 1)
           ]
         ]}
    }
  end

  def get_endpoint_query(kw), do: apply_repo_fun(:get_endpoint_query, [kw])
  def get_by(kw), do: apply_repo_fun(:get_by, [kw])
  def get_mapped_query_by_token(token), do: apply_repo_fun(:get_mapped_query_by_token, [token])

  def list_by_user_id(user_id) when is_integer(user_id) do
    if Repo.in_transaction?() do
      Endpoints.list_endpoints_by(user_id: user_id)
    else
      fetch_list_by_user_id(user_id)
    end
  end

  @impl ContextCache
  def bust_by(kw) do
    entries =
      kw
      |> Keyword.get_values(:user_id)
      |> Enum.map(&{:list_endpoints_by, [[user_id: &1]]})

    Cachex.execute(__MODULE__, fn cache ->
      Enum.reduce(entries, 0, fn key, acc -> acc + delete_and_count(cache, key) end)
    end)
  end

  defp fetch_list_by_user_id(user_id) do
    cache_key = {:list_endpoints_by, [[user_id: user_id]]}

    case Cachex.fetch(__MODULE__, cache_key, fn -> fetch_from_repo(user_id) end) do
      {_status, {:cached, endpoints}} -> endpoints
    end
  end

  defp fetch_from_repo(user_id) do
    endpoints = Repo.with_replica(fn -> Endpoints.list_endpoints_by(user_id: user_id) end)

    if endpoints == [] do
      {:ignore, {:cached, endpoints}}
    else
      {:commit, {:cached, endpoints}}
    end
  end

  defp delete_and_count(cache, key) do
    case Cachex.take(cache, key) do
      {:ok, nil} -> 0
      {:ok, _value} -> 1
    end
  end

  defp apply_repo_fun(arg1, arg2) do
    ContextCache.apply_fun(Endpoints, arg1, arg2)
  end
end
