defmodule Logflare.Teams.TeamContext do
  @moduledoc """
  Determine the current team context based on the user's selection.
  """

  alias Logflare.TeamUsers
  alias Logflare.TeamUsers.TeamUser
  alias Logflare.Teams
  alias Logflare.Teams.Team
  alias Logflare.User
  alias Logflare.Users

  @type t :: %__MODULE__{
          user: User.t(),
          team: Team.t(),
          team_user: TeamUser.t() | nil
        }
  @type error_reason :: :invalid_team_id | :not_authorized
  @type resolve_result :: {:ok, t()} | {:error, error_reason()}

  defstruct [:user, :team, :team_user]

  @spec resolve(User.t(), String.t() | nil, String.t() | nil) :: resolve_result()
  def resolve(%User{} = current_user, team_id_param, email) do
    {current_user, team_id_param, email} |> dbg

    with {:ok, role} <- parse_team_id(team_id_param) |> dbg,
         {:ok, %__MODULE__{} = ctx} <- do_resolve(current_user, role, email) do
      {:ok, ctx}
    end
  end

  @spec parse_team_id(String.t() | nil) ::
          {:ok, :owner}
          | {:ok, non_neg_integer()}
          | {:error, :invalid_team_id}
  def parse_team_id(nil), do: {:ok, :owner}
  def parse_team_id(""), do: {:ok, :owner}

  def parse_team_id(team_id_param) do
    case Integer.parse(team_id_param) do
      {team_id, ""} when team_id >= 0 -> {:ok, team_id}
      _ -> {:error, :invalid_team_id}
    end
  end

  @spec do_resolve(User.t(), :owner | non_neg_integer(), String.t()) :: resolve_result()
  defp do_resolve(%User{} = current_user, :owner, _email) do
    with {:ok, team} <- fetch_home_team(current_user) do
      {:ok, %__MODULE__{user: current_user, team: team, team_user: nil}}
    end
  end

  defp do_resolve(%User{} = current_user, team_id, email) when is_integer(team_id) do
    cond do
      team_owner?(current_user, team_id) ->
        do_resolve(current_user, :owner, email)

      team_user = TeamUsers.Cache.get_team_user_by(email: email, team_id: team_id) ->
        resolve_team_user(team_user)

      true ->
        {:error, :not_authorized}
    end
  end

  defp fetch_home_team(%User{id: user_id}), do: {:ok, Teams.get_team_by(user_id: user_id)}

  defp resolve_team_user(%TeamUser{} = team_user) do
    {1, [team_user]} = TeamUsers.touch_team_user(team_user)

    team_user = team_user |> TeamUsers.preload_defaults() |> dbg

    owner = Users.Cache.get_by_and_preload(id: team_user.team.user_id)
    {:ok, %__MODULE__{user: owner, team: team_user.team, team_user: team_user}}
  end

  defp team_owner?(%User{} = current_user, team_id) do
    user = Users.Cache.get_by_and_preload(id: current_user.id)
    user.team.id == team_id
  end
end
