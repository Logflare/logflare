defmodule LogflareWeb.TeamControllerTest do
  use LogflareWeb.ConnCase, async: true

  setup do
    insert(:plan)
    user = insert(:user)
    home_team = insert(:team, user: user)

    other_user = insert(:user)
    other_team = insert(:team, user: other_user)
    _team_user = insert(:team_user, team: other_team, email: user.email)

    %{user: user, home_team: home_team, other_team: other_team}
  end

  describe "GET /teams/switch" do
    test "stores last_switched_team_id in session and redirects with team param",
         %{conn: conn, user: user, other_team: other_team} do
      conn =
        conn
        |> login_user(user)
        |> get(~p"/teams/switch?#{[team_id: other_team.id, redirect_to: "/dashboard"]}")

      assert redirected_to(conn) =~ "/dashboard"
      assert redirected_to(conn) =~ "t=#{other_team.id}"
      assert get_session(conn, :last_switched_team_id) == other_team.id
    end

    test "redirects to home team without error",
         %{conn: conn, user: user, home_team: home_team} do
      conn =
        conn
        |> login_user(user)
        |> get(~p"/teams/switch?#{[team_id: home_team.id, redirect_to: "/dashboard"]}")

      assert redirected_to(conn) =~ "/dashboard"
    end

    test "shows error when switching to unauthorized team", %{conn: conn} do
      user = insert(:user)
      _home_team = insert(:team, user: user)

      other_user = insert(:user)
      other_team = insert(:team, user: other_user)

      conn =
        conn
        |> login_user(user)
        |> get(~p"/teams/switch?#{[team_id: other_team.id, redirect_to: "/dashboard"]}")

      assert redirected_to(conn) == "/dashboard"
      assert Phoenix.Flash.get(conn.assigns.flash, :error) == "Unable to switch to that team."
    end
  end
end
