defmodule Logflare.Partners.Cache do
  @moduledoc """
  Cache for Partners
  """

  alias Logflare.Partners
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
                 Utils.cache_limit(100_000)
               ]
               |> Enum.filter(& &1),
             expiration: Utils.cache_expiration_min()
           ]
         ]}
    }
  end

  def get_partner(id), do: apply_repo_fun(__ENV__.function, [id])
  def get_user_by_uuid(partner, token), do: apply_repo_fun(__ENV__.function, [partner, token])

  defp apply_repo_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(Partners, arg1, arg2)
  end
end
