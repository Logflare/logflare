defmodule LogflareWeb.ClusterLiveTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  import Phoenix.LiveViewTest

  setup do
    insert(:plan)
    {:ok, user: insert(:user, admin: true)}
  end
  test "successfully for admin", %{conn: conn, user: user} do
    assert {:ok, view, html} =
      conn
      |> assign(:user, user)
      |> live(~p"/admin/cluster")

    assert html =~ "#{Node.self()}"
    html = render(view)
    assert html =~ "#{Node.self()}"
  end
end
