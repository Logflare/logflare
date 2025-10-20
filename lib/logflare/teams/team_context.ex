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

  @type error_reason :: :invalid_team_id | :missing_email | :not_authorized | :missing_team
  @type resolve_result :: {:ok, t()} | {:error, error_reason()}

  defstruct [:user, :team, :team_user]

  @spec resolve(String.t() | nil, String.t() | nil) :: resolve_result()
  def resolve(team_id_param, email) when is_binary(email) do
    with {:ok, role} <- parse_team_id(team_id_param),
         {:ok, %User{} = current_user} <- fetch_user(email),
         {:ok, %__MODULE__{} = ctx} <- do_resolve(current_user, role, email) do
      {:ok, ctx}
    end
  end

  @spec parse_team_id(String.t() | nil) ::
          {:ok, :owner} | {:ok, non_neg_integer()} | {:error, :invalid_team_id}
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
    if team_owner?(current_user, team_id) do
      do_resolve(current_user, :owner, email)
    else
      case TeamUsers.Cache.get_team_user_by(email: email, team_id: team_id) do
        %TeamUser{} = team_user -> resolve_team_user(team_user)
        _ -> {:error, :not_authorized}
      end
    end
  end

  @spec fetch_user(String.t()) :: {:ok, User.t()} | {:error, :not_authorized}
  defp fetch_user(email) do
    case Users.Cache.get_by_and_preload(email: email) do
      %User{} = user -> {:ok, user}
      _ -> {:error, :not_authorized}
    end
  end

  defp fetch_home_team(%User{team: %Team{} = team}), do: {:ok, Teams.preload_team_users(team)}

  defp fetch_home_team(%User{id: user_id}) do
    case Teams.get_team_by(user_id: user_id) do
      %Team{} = team -> {:ok, Teams.preload_team_users(team)}
      _ -> {:error, :missing_team}
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

  defp team_owner?(%User{team: %Team{id: id}}, team_id) when id == team_id, do: true

  defp team_owner?(%User{id: user_id}, team_id) do
    case Teams.get_team_by(id: team_id) do
      %Team{user_id: ^user_id} -> true
      _ -> false
    end
  end
end
