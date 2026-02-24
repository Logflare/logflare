defmodule Logflare.Endpoints.Cache do
  @moduledoc """
  Cachex for Endpoints context.
  """

  alias Logflare.Endpoints
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

  defp apply_repo_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(Endpoints, arg1, arg2)
  end
end
