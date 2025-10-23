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

  @type error_reason :: :invalid_team_id | :not_authorized | :missing_team
  @type resolve_result :: {:ok, t()} | {:error, error_reason()}
  @type team_id_param :: String.t() | non_neg_integer() | nil

  defstruct [:user, :team, :team_user]

  @spec resolve(team_id_param(), String.t()) :: resolve_result()
  def resolve(team_id_param, email) when is_binary(email) do
    with {:ok, role} <- parse_team_id(team_id_param),
         {:ok, team, role} <- verify_team(email, role),
         {:ok, %__MODULE__{} = ctx} <- do_resolve(team, role, email) do
      {:ok, ctx}
    end
  end

  @spec parse_team_id(String.t() | nil) ::
          {:ok, :owner} | {:ok, non_neg_integer()} | {:error, :invalid_team_id}
  def parse_team_id(nil), do: {:ok, :owner}
  def parse_team_id(""), do: {:ok, :owner}

  def parse_team_id(team_id_param) when is_binary(team_id_param) do
    case Integer.parse(team_id_param) do
      {team_id, ""} when team_id >= 0 -> {:ok, team_id}
      _ -> {:error, :invalid_team_id}
    end
  end

  def parse_team_id(team_id_param) when is_integer(team_id_param), do: {:ok, team_id_param}

  def parse_team_id(_), do: {:error, :invalid_team_id}

  defp do_resolve(%Team{} = team, :owner, _email) do
    team = Logflare.Teams.preload_user(team)
    {:ok, %__MODULE__{user: team.user, team: team, team_user: nil}}
  end

  defp do_resolve(_team, team_id, email) when is_integer(team_id) do
    case TeamUsers.Cache.get_team_user_by(email: email, team_id: team_id) do
      %TeamUser{} = team_user -> resolve_team_user(team_user)
      _ -> {:error, :not_authorized}
    end
  end

  defp verify_team(email, :owner) when is_binary(email) do
    case Users.Cache.get_by_and_preload(email: email) do
      %User{} = user ->
        {:ok, user.team, :owner}

      _ ->
        {:error, :not_authorized}
    end
  end

  defp verify_team(email, team_id) when is_binary(email) do
    cond do
      email_is_team_owner?(email, team_id) ->
        verify_team(email, :owner)

      team_user = fetch_team_user(email, team_id) ->
        team_user = team_user |> TeamUsers.preload_defaults()
        {:ok, team_user.team, team_id}

      true ->
        {:error, :not_authorized}
    end
  end

  defp resolve_team_user(%TeamUser{} = team_user) do
    case TeamUsers.touch_team_user(team_user) do
      {1, [touched]} ->
        touched
        |> TeamUsers.preload_defaults()
        |> build_member_context()

      _ ->
        {:error, :not_authorized}
    end
  end

  defp build_member_context(%TeamUser{} = team_user) do
    case Users.Cache.get_by_and_preload(id: team_user.team.user_id) do
      %User{} = owner ->
        {:ok,
         %__MODULE__{
           user: owner,
           team: Teams.preload_team_users(team_user.team),
           team_user: team_user
         }}

      _ ->
        {:error, :not_authorized}
    end
  end

  defp email_is_team_owner?(email, team_id) do
    case Users.Cache.get_by_and_preload(email: email) do
      %User{} = user ->
        user.team.id == team_id

      _ ->
        false
    end
  end

  defp fetch_team_user(email, team_id) do
    TeamUsers.Cache.get_team_user_by(email: email, team_id: team_id)
  end
end
