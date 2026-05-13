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

    socket = %Phoenix.LiveView.Socket{endpoint: LogflareWeb.Endpoint, router: LogflareWeb.Router}
    session = %{"current_email" => user.email, "last_switched_team_id" => other_team.id}

    assert {:cont, socket} = AuthLive.on_mount(:default, %{}, session, socket)
    assert socket.assigns.team.id == other_team.id
  end
end
