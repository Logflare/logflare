defmodule LogflareWeb.Plugs.SetTeamIfNilTest do
  use LogflareWeb.ConnCase, async: true

  alias LogflareWeb.Plugs.SetTeamIfNil
  alias Logflare.Teams.Team
  alias Logflare.Repo

  @opts []

  describe "init/1" do
    test "returns []" do
      assert SetTeamIfNil.init(@opts) == []
    end
  end

  describe "call/2" do
    setup do
      user = insert(:user)
      %{user: user}
    end

    test "creates team when user has no team", %{conn: conn, user: user} do
      user = Repo.preload(user, :team)

      assert user.team == nil

      conn =
        conn
        |> login_user(user)
        |> SetTeamIfNil.call(@opts)

      user = Repo.preload(user, :team, force: true)

      assert %Team{} = team = user.team
      assert team.user_id == user.id
      assert is_binary(team.name)
      assert String.length(team.name) > 0
      assert conn.assigns.user.id == user.id

      assert Repo.preload(user, :team, force: true).team == team
    end

    test "does not create team when user already has a team", %{conn: conn, user: user} do
      team = insert(:team, user: user)

      user = Repo.preload(user, :team)

      assert user.team.id == team.id

      conn
      |> login_user(user)
      |> SetTeamIfNil.call(@opts)

      assert Repo.preload(user, :team, force: true).team.id == team.id
    end
  end
end
