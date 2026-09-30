defmodule Logflare.Users.Cache do
  @moduledoc """
  Cache for users.
  """

  use Logflare.ContextCache, refresh_ahead: true

  alias Logflare.Cache.CachexOps
  alias Logflare.Users

  def child_spec(_) do
    CachexOps.child_spec(__MODULE__,
      limit: 100_000,
      ttl: to_timeout(hour: 3),
      purge_interval: to_timeout(minute: 10),
      warmer: Users.CacheWarmer
    )
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
