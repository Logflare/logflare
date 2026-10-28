defmodule Logflare.TeamUsers.Cache do
  @moduledoc """
  Cache for TeamUsers.
  """

  use Logflare.ContextCache

  alias Logflare.Cache.CachexOps
  alias Logflare.TeamUsers

  def child_spec(_), do: CachexOps.child_spec(__MODULE__, limit: 100_000)

  def get_team_user(id), do: apply_repo_fun({:get_team_user, 1}, [id])
  def get_team_user!(id), do: apply_repo_fun(:get_team_user!, [id])
  def get_team_user_and_preload(id), do: apply_repo_fun(:get_team_user_and_preload, [id])
  def preload_defaults(team_user), do: apply_repo_fun(:preload_defaults, [team_user])
  def get_team_user_by(keyword), do: apply_repo_fun(:get_team_user_by, [keyword])

  defp apply_repo_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(TeamUsers, arg1, arg2)
  end
end
