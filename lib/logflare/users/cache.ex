defmodule Logflare.Users.Cache do
  @moduledoc """
  Cache for users.
  """

  @behaviour Logflare.Cache
  @behaviour Logflare.ContextCache

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

  @impl Logflare.Cache
  def healthy?, do: CachexOps.healthy?(__MODULE__)

  @impl Logflare.Cache
  def stats, do: CachexOps.stats(__MODULE__)

  @impl Logflare.Cache
  def reset, do: CachexOps.reset(__MODULE__)

  @impl Logflare.ContextCache
  def fetch(key, getter), do: CachexOps.fetch(__MODULE__, key, getter)

  @impl Logflare.ContextCache
  def update(key, value), do: CachexOps.update(__MODULE__, key, value)

  @impl Logflare.ContextCache
  def keys_to_bust(kw), do: CachexOps.keys_to_bust(__MODULE__, kw)

  @impl Logflare.ContextCache
  def delete_keys(keys), do: CachexOps.delete_keys(__MODULE__, keys)

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
