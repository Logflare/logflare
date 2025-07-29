defmodule Logflare.TeamsTest do
  @moduledoc false
  use Logflare.DataCase

  alias Logflare.Teams
  alias Logflare.Teams.Team
  alias Logflare.User

  test "list_teams/0 fetches data correctly" do
    teams = insert_list(2, :team)
    expected = teams |> Enum.map(& &1.id) |> Enum.sort()
    result = Teams.list_teams() |> Enum.map(& &1.id) |> Enum.sort()

    assert expected == result
  end

  test "get_team/1 fetches data correctly" do
    %{id: team_id} = insert(:team)

    assert %Team{id: ^team_id} = Teams.get_team!(team_id)
  end

  test "get_team_by/1 fetches data correctly" do
    %{id: team_id, name: name} = insert(:team)

    assert %Team{id: ^team_id} = Teams.get_team_by(name: name)
  end

  test "get_home_team/1 returns a paying user's home team" do
    # owned by different user
    home_team = insert(:team)
    user = insert(:user, team: home_team)
    team_user = insert(:team_user, email: user.email)

    assert Teams.get_home_team(team_user).id == home_team.id

    Repo.delete!(user)

    assert Teams.get_home_team(team_user) == nil
  end

  test "get_home_team/1 returns nil when user does not exist" do
    team_user = insert(:team_user, email: "")

    assert Teams.get_home_team(team_user) == nil
  end

  test "preload_user/1 preloads :user" do
    %{id: user_id} = user = insert(:user)
    team = insert(:team, user: user)

    assert %{user: %User{id: ^user_id}} = Teams.preload_user(team)
    assert Teams.preload_user(nil) == nil
  end

  test "preload_team_users/1 preloads :team_users" do
    team = insert(:team, user: insert(:user))
    %{email: user_email} = user = insert(:user)
    insert(:team_user, team: team, email: user.email)

    assert %{team_users: [%{email: ^user_email}]} = Teams.preload_team_users(team)
    assert Teams.preload_team_users(nil) == nil
  end

  test "preload_fields/1 preloads all fields in the atom list" do
    %{id: user_id} = user = insert(:user)
    team = insert(:team, user: user)
    %{email: user_email} = user = insert(:user)
    insert(:team_user, team: team, email: user.email)

    assert %{user: %User{id: ^user_id}, team_users: [%{email: ^user_email}]} =
             Teams.preload_fields(team, [:user, :team_users])

    assert Teams.preload_fields(nil, [:user]) == nil
  end

  test "create_team/2 creates a new team in the database" do
    name = TestUtils.random_string()
    user = insert(:user)
    assert {:ok, %Team{name: ^name}} = Teams.create_team(user, %{name: name})
  end

  test "update_team/2 updates an existing team" do
    name = TestUtils.random_string()
    %{id: id} = team = insert(:team)
    assert {:ok, %{id: ^id, name: ^name}} = Teams.update_team(team, %{name: name})
  end

  test "delete_team/1 deletes an existing teams" do
    %{id: id} = team = insert(:team)
    assert {:ok, %{id: ^id}} = Teams.delete_team(team)

    assert_raise Ecto.StaleEntryError, fn ->
      Teams.delete_team(team)
    end
  end

  test "change_team/1 returns a team changeset" do
    %{id: id} = team = insert(:team)
    assert %Ecto.Changeset{data: %Team{id: ^id}} = Teams.change_team(team)
  end

  test "list_teams_by_user_access/1 lists all teams of a given user" do
    insert(:team, user: build(:user))
    user = insert(:user)
    insert_list(2, :team_user, provider_uid: user.provider_uid)

    # 2 items, :team_users preloaded
    assert [%{team_users: [_ | _]}, _] = Teams.list_teams_by_user_access(user)
  end

  test "get_team_by_user_access/2 gets a team for a given user and token" do
    user = insert(:user)
    team = insert(:team)
    _team_user = insert(:team_user, provider_uid: user.provider_uid, team: team)
    insert_list(2, :team_user)

    assert team.id == Teams.get_team_by_user_access(user, team.token).id
  end
end
