defmodule Logflare.Users.Cache do
  @moduledoc """
  Cache for users.
  """

  alias Logflare.Users
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

  def get(id), do: apply_repo_fun(__ENV__.function, [id])
  def get_by(keyword), do: apply_repo_fun(__ENV__.function, [keyword])

  def get_by_and_preload(keyword), do: apply_repo_fun(__ENV__.function, [keyword])
  def preload_defaults(user), do: apply_repo_fun(__ENV__.function, [user])

  defp apply_repo_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(Users, arg1, arg2)
  end
end
