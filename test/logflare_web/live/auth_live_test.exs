defmodule LogflareWeb.AuthLiveTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  alias LogflareWeb.AuthLive

  test "on_mount redirects to login when session has no current_email" do
    socket = %Phoenix.LiveView.Socket{endpoint: LogflareWeb.Endpoint, router: LogflareWeb.Router}

    assert {:halt, socket} = AuthLive.on_mount(:default, %{}, %{}, socket)
    assert {:redirect, %{to: "/auth/login"}} = socket.redirected
  end
end
