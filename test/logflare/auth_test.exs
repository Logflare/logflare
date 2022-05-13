defmodule Logflare.AuthTest do
  use Logflare.DataCase
  alias Logflare.Auth
  alias Logflare.Auth.ApiKey
  alias Logflare.Factory
  alias Logflare.Teams.Team
  alias Logflare.TeamUsers.TeamUser
  alias Logflare.User
  setup do
    user  = Factory.insert(:user)
    [user: user, team: Factory.insert(:team, user: user) ]
  end

  describe "api access tokens" do
    test "can create api key", %{user: user, team: team} do
      {:ok, %ApiKey{}} = Auth.create_access_token(user)
      {:ok, %ApiKey{}} = Auth.create_access_token(team)
    end
    test "can revoke access tokens", %{user: user} do
      key = access_token_fixture(user)
      :ok = Auth.revoke_access_token(key)
    end
    test "verify access tokens", %{user: %{id: user_id} = user, team: %{id: team_id} = team} do
      # api key without a team
      key = access_token_fixture(user)
      {:ok, %User{}, nil} = Auth.verify_access_token(key)

      # api key with a team
      key = access_token_fixture(team)
      {:ok, %User{}, %Team{}} = Auth.verify_access_token(key)
    end
  end
  defp access_token_fixture(user_or_team) do
    {:ok, key} = Auth.create_access_token(user_or_team)
    key
  end
end
