defmodule Logflare.TeamUsers.Cache do
  @moduledoc """
  Cache for TeamUsers.
  """

  @behaviour Logflare.Cache
  @behaviour Logflare.ContextCache

  alias Logflare.Cache.CachexOps
  alias Logflare.TeamUsers

  def child_spec(_), do: CachexOps.child_spec(__MODULE__, limit: 100_000)

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

  def get_team_user(id), do: apply_repo_fun({:get_team_user, 1}, [id])
  def get_team_user!(id), do: apply_repo_fun(:get_team_user!, [id])
  def get_team_user_and_preload(id), do: apply_repo_fun(:get_team_user_and_preload, [id])
  def preload_defaults(team_user), do: apply_repo_fun(:preload_defaults, [team_user])
  def get_team_user_by(keyword), do: apply_repo_fun(:get_team_user_by, [keyword])

  defp apply_repo_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(TeamUsers, arg1, arg2)
  end
end
