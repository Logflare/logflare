defmodule Logflare.TeamUsers.Cache do
  @moduledoc """
  Cache for TeamUsers.
  """

  alias Logflare.TeamUsers
  alias Logflare.Utils

  def child_spec(_) do
    stats = Application.get_env(:logflare, :cache_stats, false)

    %{
      id: __MODULE__,
      start:
        {Cachex, :start_link,
         [__MODULE__, [stats: stats, expiration: Utils.cache_expiration_min(), limit: 100_000]]}
    }
  end

  def get_team_user(id), do: apply_repo_fun({:get_team_user, 1}, [id])
  def get_team_user!(id), do: apply_repo_fun(:get_team_user!, [id])
  def get_team_user_and_preload(id), do: apply_repo_fun(:get_team_user_and_preload, [id])
  def preload_defaults(team_user), do: apply_repo_fun(:preload_defaults, [team_user])
  def get_team_user_by(keyword), do: apply_repo_fun(:get_team_user_by, [keyword])

  defp apply_repo_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(TeamUsers, arg1, arg2)
  end
end
