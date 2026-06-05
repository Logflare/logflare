defmodule LogflareWeb.AuthLiveTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  alias LogflareWeb.AuthLive

  setup do
    insert(:plan)
    :ok
  end

  test "on_mount redirects to login when session has no current_email" do
    socket = %Phoenix.LiveView.Socket{endpoint: LogflareWeb.Endpoint, router: LogflareWeb.Router}

    assert {:halt, socket} = AuthLive.on_mount(:default, %{}, %{}, socket)
    assert {:redirect, %{to: "/auth/login"}} = socket.redirected
  end

  test "on_mount uses last_switched_team_id from session when param absent" do
    user = insert(:user)
    _home_team = insert(:team, user: user)

    other_user = insert(:user)
    other_team = insert(:team, user: other_user)
    _team_user = insert(:team_user, team: other_team, email: user.email)

    socket = %Phoenix.LiveView.Socket{
      endpoint: LogflareWeb.Endpoint,
      private: %{lifecycle: %Phoenix.LiveView.Lifecycle{}},
      router: LogflareWeb.Router
    }

    session = %{"current_email" => user.email, "last_switched_team_id" => other_team.id}

    assert {:cont, socket} = AuthLive.on_mount(:default, %{}, session, socket)
    assert socket.assigns.team.id == other_team.id
  end

  test "assign_context_by_team_id raises when team context cannot be resolved" do
    user = insert(:user)
    team = insert(:team, user: user)
    inaccessible_team = insert(:team)

    socket = %Phoenix.LiveView.Socket{
      endpoint: LogflareWeb.Endpoint,
      assigns: %{user: user, team: team, team_user: nil},
      router: LogflareWeb.Router
    }

    assert_raise RuntimeError, fn ->
      AuthLive.assign_context_by_team_id(socket, inaccessible_team.id, user.email)
    end
  end

  describe "ensure team param hook" do
    setup %{conn: conn} do
      user = insert(:user)
      team = insert(:team, user: user)

      [conn: login_user(conn, user), team: team]
    end

    test "adds t= to home team dashboard route", %{conn: conn, team: team} do
      assert {:error, {:live_redirect, %{to: path}}} = live(conn, ~p"/dashboard")

      assert path == ~p"/dashboard?t=#{team.id}"
    end

    test "adds t= to team user dashboard route", %{conn: conn, team: team} do
      other_team = insert(:team)
      team_user = insert(:team_user, team: other_team)

      assert {:error, {:live_redirect, %{to: path}}} =
               conn
               |> login_user(team.user, team_user)
               |> live(~p"/dashboard")

      assert path == ~p"/dashboard?t=#{other_team.id}"
    end

    test "preserves existing query params when adding t=", %{conn: conn, team: team} do
      assert {:error, {:live_redirect, %{to: path}}} =
               live(conn, ~p"/dashboard?#{[from: "nav"]}")

      uri = URI.parse(path)

      assert uri.path == "/dashboard"
      assert URI.decode_query(uri.query) == %{"from" => "nav", "t" => to_string(team.id)}
    end
  end
end
