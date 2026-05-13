defmodule LogflareWeb.TeamSwitcherTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  setup %{conn: conn} do
    insert(:plan)
    user = insert(:user)
    team = insert(:team, user: user)
    user = %{user | team: team}
    conn = conn |> login_user(user)

    {:ok, user: user, conn: conn}
  end

  describe "team_switcher/1" do
    test "renders dropdown when user has multiple teams", %{conn: conn, user: user} do
      team2 = insert(:team, %{name: "Team B"})
      _team_user2 = insert(:team_user, team: team2, email: user.email)

      conn
      |> visit("/account/edit")
      |> assert_has("#team-switcher a", text: user.team.name)
      |> assert_has("#team-switcher a", text: user.team.name <> " " <> "home team")
      |> assert_has("#team-switcher a", text: team2.name)
    end

    test "renders current team name bold", %{conn: conn, user: user} do
      team2 = insert(:team, %{name: "Team B"})
      _team_user2 = insert(:team_user, team: team2, email: user.email)

      conn
      |> visit("/account/edit")
      |> assert_has(~s|#team-switcher a[class~=tw-font-bold]|, text: user.team.name)
    end

    test "renders team name when user has single team", %{conn: conn, user: user} do
      conn
      |> visit("/account/edit")
      |> assert_has("#team-switcher span", text: user.team.name)
    end

    test "preserves query params in team switcher links", %{conn: conn, user: user} do
      team2 = insert(:team)
      _team_user2 = insert(:team_user, team: team2, email: user.email)

      conn
      |> visit("/account/edit?key=test")
      |> assert_has(
        ~s|#team-switcher a[href="/teams/switch?team_id=#{team2.id}&redirect_to=%2Faccount%2Fedit%3Fkey%3Dtest"]|
      )
    end
  end
end
