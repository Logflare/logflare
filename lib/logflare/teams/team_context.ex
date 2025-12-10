defmodule Logflare.Teams.TeamContext do
  @moduledoc """
  Determine the current team context based on the user's selection.

  Takes `email`, usually from the logged in session, and `team_id_param`, typically from the URL query params.

  If `team_id_param` is nil or empty, the user's home team is used.
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

  @type error_reason :: :invalid_team_id | :not_authorized | :team_not_found
  @type resolve_result :: {:ok, t()} | {:error, error_reason()}
  @type team_id_param :: String.t() | non_neg_integer() | nil

  defstruct [:user, :team, :team_user]

  @spec resolve(team_id_param(), String.t()) :: resolve_result()
  def resolve(team_id_param, email) when is_binary(email) do
    with {:ok, role} <- parse_team_id(team_id_param),
         {:ok, team, user_or_team_user} <- verify_team_access(email, role),
         team <- Teams.preload_team_users(team),
         {:ok, %__MODULE__{} = ctx} <- do_resolve(team, user_or_team_user) do
      {:ok, ctx}
    end
  end

  @spec parse_team_id(team_id_param()) ::
          {:ok, :home_team} | {:ok, non_neg_integer()} | {:error, :invalid_team_id}
  def parse_team_id(nil), do: {:ok, :home_team}
  def parse_team_id(""), do: {:ok, :home_team}
  def parse_team_id(team_id_param) when is_integer(team_id_param), do: {:ok, team_id_param}

  def parse_team_id(team_id_param) when is_binary(team_id_param) do
    case Integer.parse(team_id_param) do
      {team_id, ""} when team_id >= 0 -> {:ok, team_id}
      _ -> {:error, :invalid_team_id}
    end
  end

  def parse_team_id(_), do: {:error, :invalid_team_id}

  def is_team_owner?(%__MODULE__{team: team, user: user, team_user: nil}),
    do: team_owner?(team, user.email)

  def is_team_owner?(%__MODULE__{}), do: false

  defp do_resolve(%Team{} = team, %TeamUser{} = team_user) do
    user = team.user |> Logflare.Users.preload_defaults()
    team_user = TeamUsers.preload_defaults(team_user)
    {:ok, %__MODULE__{user: user, team: team, team_user: team_user}}
  end

  defp do_resolve(%Team{} = team, %User{} = _user) do
    user = team.user |> Logflare.Users.preload_defaults()
    {:ok, %__MODULE__{user: user, team: team, team_user: nil}}
  end

  # When no team id is provided, default to home team if available.
  defp verify_team_access(email, :home_team) when is_binary(email) do
    case Users.get_by_and_preload(email: email) do
      %User{} = user ->
        team = user.team |> Teams.preload_user()
        {:ok, team, user}

      nil ->
        case TeamUsers.list_team_users_by(email: email) |> Enum.sort_by(& &1.inserted_at) do
          [%TeamUser{} = team_user | _] ->
            team =
              Teams.get_team_by(id: team_user.team_id)
              |> Teams.preload_user()

            {:ok, team, team_user}

          _ ->
            {:error, :not_authorized}
        end
    end
  end

  defp verify_team_access(email, team_id) when is_binary(email) do
    team =
      Teams.get_team_by(id: team_id)
      |> Teams.preload_user()

    cond do
      is_nil(team) ->
        {:error, :team_not_found}

      team_owner?(team, email) ->
        {:ok, team, team.user}

      team_user = fetch_team_user(team, email) ->
        {:ok, team, team_user}

      true ->
        {:error, :not_authorized}
    end
  end

  defp team_owner?(team, email), do: team.user.email == email

  defp fetch_team_user(team, email) do
    TeamUsers.get_team_user_by(email: email, team_id: team.id)
  end
end
