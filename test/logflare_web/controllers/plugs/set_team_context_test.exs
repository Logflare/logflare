defmodule LogflareWeb.Plugs.SetTeamContextTest do
  use LogflareWeb.ConnCase, async: true
  alias LogflareWeb.Plugs.SetTeamContext
  @opts []
  setup do
    insert(:plan)
    [conn: Phoenix.ConnTest.build_conn()]
  end

  describe "call/2 without authentication" do
    test "unauthenticated assigns user to nil", %{conn: conn} do
      conn =
        conn
        |> Plug.Test.init_test_session(%{})
        |> SetTeamContext.call(@opts)

      assert conn.assigns.user == nil
    end
  end

  describe "call/2 authenticated as user" do
    test "assigns user and team", %{conn: conn} do
      user = insert(:user)
      team = insert(:team, user: user)

      conn =
        conn
        |> login_user(user)
        |> SetTeamContext.call(@opts)

      assert conn.assigns.user.id == user.id
      assert conn.assigns.team.id == team.id
      assert is_list(conn.assigns.teams)
    end

    test "switches to different team as team_user", %{conn: conn} do
      user = insert(:user)
      _team1 = insert(:team, user: user)

      other_user = insert(:user)
      team2 = insert(:team, user: other_user)

      _team_user = insert(:team_user, team: team2, email: user.email)

      conn =
        conn
        |> login_user(user)
        |> Map.put(:params, %{"t" => to_string(team2.id)})
        |> SetTeamContext.call(@opts)

      assert conn.assigns.user.id == other_user.id
      assert conn.assigns.team.id == team2.id
      assert is_list(conn.assigns.teams)
    end
  end

  describe "call/2 authenticated as team_user" do
    test "assigns user, team, and team_user", %{conn: conn} do
      user = insert(:user)
      team = insert(:team, user: user)
      team_user = insert(:team_user, team: team)

      conn =
        conn
        |> Plug.Test.init_test_session(%{current_email: team_user.email})
        |> SetTeamContext.call(@opts)

      assert conn.assigns.user.id == user.id
      assert conn.assigns.team.id == team.id
      assert conn.assigns.team_user.id == team_user.id
      assert is_list(conn.assigns.teams)
    end

    test "switches to different team with valid team_id param", %{conn: conn} do
      user1 = insert(:user)
      team1 = insert(:team, user: user1)

      user2 = insert(:user)
      team2 = insert(:team, user: user2)

      team_user = insert(:team_user, team: team1)
      _team_user2 = insert(:team_user, team: team2, email: team_user.email)

      conn =
        conn
        |> Plug.Test.init_test_session(%{current_email: team_user.email})
        |> Map.put(:params, %{"t" => to_string(team2.id)})
        |> SetTeamContext.call(@opts)

      assert conn.assigns.user.id == user2.id
      assert conn.assigns.team.id == team2.id
      assert conn.assigns.team_user.email == team_user.email
      assert is_list(conn.assigns.teams)
    end
  end

  describe "call/2 handles errors" do
    test "redirects on team_not_found", %{conn: conn} do
      user = insert(:user)
      _team = insert(:team, user: user)

      conn =
        conn
        |> login_user(user)
        |> Map.put(:params, %{"t" => "999999"})
        |> SetTeamContext.call(@opts)

      assert conn.status == 403
      assert conn.halted
    end

    test "redirects on not_authorized", %{conn: conn} do
      user1 = insert(:user)
      _team1 = insert(:team, user: user1)

      user2 = insert(:user)
      team2 = insert(:team, user: user2)

      conn =
        conn
        |> login_user(user1)
        |> Map.put(:params, %{"t" => to_string(team2.id)})
        |> SetTeamContext.call(@opts)

      assert conn.status == 403
      assert conn.halted
    end

    test "preserves other query params when redirecting on invalid team_id", %{conn: conn} do
      user = insert(:user)
      _team = insert(:team, user: user)

      conn =
        conn
        |> login_user(user)
        |> Map.put(:params, %{"t" => "invalid_id", "foo" => "bar"})
        |> Map.put(:query_params, %{"t" => "invalid_id", "foo" => "bar"})
        |> Map.put(:request_path, "/sources")
        |> SetTeamContext.call(@opts)

      assert conn.status == 302
      assert conn.halted
      assert redirected_to(conn) == "/sources?foo=bar"
    end
  end
end
