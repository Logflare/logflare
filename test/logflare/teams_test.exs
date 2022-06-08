defmodule Logflare.TeamsTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.{Teams, User}

  describe "teams" do
    alias Logflare.Teams.Team
    @valid_attrs %{name: "some name"}
    @update_attrs %{name: "diff name"}
    test "list_teams/0, get_team/1, get_team_by/1 fetches data correctly" do
      team = insert(:team)
      id = team.id
      assert [%Team{}] = Teams.list_teams()
      assert %Team{id: ^id} = Teams.get_team!(team.id)
      assert %Team{id: ^id} = Teams.get_team_by(name: team.name)

      assert %{user: %User{}, team_users: []} =
               team |> Teams.preload_user() |> Teams.preload_team_users()
    end

    test "get_home_team/1 returns a paying user's home team" do
      # owned by different user
      home_team = insert(:team)
      user = insert(:user, team: home_team)
      team_user = insert(:team_user, email: user.email)
      assert Teams.get_home_team(team_user).id == home_team.id
    end

    test "create_team/2, update_team/2, delete_team/1" do
      user = insert(:user)
      assert {:ok, %Team{} = team} = Teams.create_team(user, @valid_attrs)
      assert team.name == @valid_attrs.name

      assert {:ok, %Team{} = team} = Teams.update_team(team, @update_attrs)
      assert team.name == @update_attrs.name

      assert {:ok, %Team{} = team} = Teams.delete_team(team)

      assert_raise Ecto.StaleEntryError, fn ->
        Teams.delete_team(team)
      end
    end
  end
end
