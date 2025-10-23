defmodule Logflare.Teams.TeamContextTest do
  use Logflare.DataCase

  alias Logflare.Teams.TeamContext

  setup do
    user = insert(:user)
    team = insert(:team, user: user)

    other_team = insert(:team)
    team_user = insert(:team_user, team: team)

    {:ok, user: user, team: team, team_user: team_user, other_team: other_team}
  end

  describe "team context: team owner email" do
    test "resolve with no team_id", %{user: user, team: team} do
      {:ok, context} = TeamContext.resolve(nil, user.email)

      assert context.user.id == user.id
      assert context.team.id == team.id
      assert context.team_user == nil
    end

    test "resolve with own team_id", %{user: user, team: team} do
      {:ok, context} = TeamContext.resolve(team.id, user.email)

      assert context.user.id == user.id
      assert context.team.id == team.id
      assert context.team_user == nil
    end

    test "resolve with other team_id", %{user: user, other_team: other_team} do
      assert {:error, :not_authorized} = TeamContext.resolve(other_team.id, user.email)
    end
  end

  describe "team context when email is team user" do
    test "resolve with authorized team_id", %{user: user, team: team, team_user: team_user} do
      {:ok, context} = TeamContext.resolve(team.id, team_user.email)

      assert context.user.id == user.id
      assert context.team.id == team.id
      assert context.team_user.id == team_user.id
    end

    test "resolve with forbidden team_id", %{other_team: other_team, team_user: team_user} do
      assert {:error, :not_authorized} = TeamContext.resolve(other_team.id, team_user.email)
    end
  end
end
