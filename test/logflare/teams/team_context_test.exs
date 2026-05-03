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

  describe "team context: multiple teams for same email" do
    test "bug: does not raise Ecto.MultipleResultsError when user is invited to multiple teams and no team_id is provided and user has no home team" do
      [team1, team2] = insert_pair(:team)
      shared_email = "user@example.com"

      insert(:team_user, email: shared_email, team: team1)
      insert(:team_user, email: shared_email, team: team2)

      assert {:ok, %{team: team, team_user: team_user}} = TeamContext.resolve(nil, shared_email)
      assert team.id == team1.id
      assert team_user.team_id == team1.id
    end

    test "user is invited to one team and no team_id is provided and user has no home team" do
      [team1, _team2] = insert_pair(:team)
      shared_email = "user@example.com"

      insert(:team_user, email: shared_email, team: team1)

      assert {:ok, %{team: team, team_user: team_user}} = TeamContext.resolve(nil, shared_email)
      assert team.id == team1.id
      assert team_user.team_id == team1.id
    end
  end

  describe "team context for members of superadmin team" do
    test "should resolve to superadmin team even when user has a home team" do
      admin = insert(:user, admin: true)
      admin_team = insert(:team, user: admin)
      user = insert(:user)
      home_team = insert(:team, user: user)
      admin_team_user = insert(:team_user, email: user.email, team: admin_team)

      # if no query param, resolve to home team
      assert {:ok, %TeamContext{user: user, team: team, team_user: team_user}} =
               TeamContext.resolve(nil, user.email)

      assert team.id == home_team.id
      assert team_user == nil
      assert user.admin == false
      # if admin_team query param, resolve to admin team
      assert {:ok, %TeamContext{user: user, team: team, team_user: team_user}} =
               TeamContext.resolve(admin_team.id, user.email)

      assert team.id == admin_team.id
      assert user.admin
      assert team_user.id == admin_team_user.id
    end
  end

  describe "team context: user with home team and invited to multiple teams" do
    setup do
      user = insert(:user)
      home_team = insert(:team, user: user)

      [invited_team1, invited_team2] = insert_pair(:team)
      team_user1 = insert(:team_user, email: user.email, team: invited_team1)
      team_user2 = insert(:team_user, email: user.email, team: invited_team2)

      {:ok,
       user: user,
       home_team: home_team,
       invited_team1: invited_team1,
       invited_team2: invited_team2,
       team_user1: team_user1,
       team_user2: team_user2}
    end

    test "resolve with no team_id returns home team of the provided email", %{
      user: user,
      home_team: home_team
    } do
      {:ok, context} = TeamContext.resolve(nil, user.email)

      assert context.user.id == user.id
      assert context.team.id == home_team.id
      assert context.team_user == nil
    end

    test "resolve with home team_id returns home team of the provided email", %{
      user: user,
      home_team: home_team
    } do
      {:ok, context} = TeamContext.resolve(home_team.id, user.email)

      assert context.user.id == user.id
      assert context.team.id == home_team.id
      assert context.team_user == nil
    end

    test "resolve with invited team_id returns correct team_user", %{
      user: user,
      invited_team1: invited_team1,
      team_user1: team_user1,
      invited_team2: invited_team2
    } do
      {:ok, context} = TeamContext.resolve(invited_team1.id, user.email)

      assert context.user.id == invited_team1.user.id
      assert context.team.id == invited_team1.id
      assert context.team_user.id == team_user1.id
      # resolve different team context
      {:ok, team_context_2} = TeamContext.resolve(invited_team2.id, user.email)
      assert context != team_context_2
    end

    test "resolve with forbidden team_id returns error", %{user: user} do
      forbidden_team = insert(:team)
      assert {:error, :not_authorized} = TeamContext.resolve(forbidden_team.id, user.email)
    end
  end

  describe "team_owner?/1" do
    test "returns true for user accessing their own home team", %{user: user, team: team} do
      {:ok, context} = TeamContext.resolve(team.id, user.email)

      assert context.team_user == nil
      assert TeamContext.team_owner?(context) == true
    end

    test "returns false for user accessing a team they are a member of", %{user: user} do
      invited_team = insert(:team)
      insert(:team_user, email: user.email, team: invited_team)
      {:ok, context} = TeamContext.resolve(invited_team.id, user.email)

      assert context.team_user != nil
      assert TeamContext.team_owner?(context) == false
    end
  end
end
