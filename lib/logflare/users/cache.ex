defmodule Logflare.Users.Cache do
  @moduledoc """
  Cache for users.
  """

  alias Logflare.Users
  alias Logflare.Utils
  import Cachex.Spec

  def child_spec(_) do
    %{
      id: __MODULE__,
      start:
        {Cachex, :start_link,
         [
           __MODULE__,
           [
             warmers: [
               warmer(required: false, module: Users.CacheWarmer, name: Users.CacheWarmer)
             ],
             hooks:
               [
                 Utils.cache_stats(),
                 Utils.cache_limit(100_000)
               ]
               |> Enum.filter(& &1),
             expiration: Utils.cache_expiration_min(180, 10)
           ]
         ]}
    }
  end

  def update(user),
    do: Logflare.ContextCache.update(Users, :get, [user.id], user)

  def get(id), do: apply_repo_fun(__ENV__.function, [id])

  def get_by(keyword), do: apply_repo_fun(__ENV__.function, [keyword])
  def get_by_and_preload(keyword), do: apply_repo_fun(__ENV__.function, [keyword])
  def preload_defaults(user), do: apply_repo_fun(__ENV__.function, [user])
  def preload_sources(user), do: apply_repo_fun(__ENV__.function, [user])

  defp apply_repo_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(Users, arg1, arg2)
  end
end
