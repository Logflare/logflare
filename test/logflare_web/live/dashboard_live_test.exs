defmodule LogflareWeb.DashboardLiveTest do
  @moduledoc false
  use LogflareWeb.ConnCase, async: true

  setup %{conn: conn} do
    insert(:plan)
    user = insert(:user)
    team = insert(:team, user: user)
    user = %{user | team: team}
    conn = conn |> put_session(:user_id, user.id) |> assign(:user, user)

    {:ok, user: user, conn: conn}
  end

  describe "Dashboard Live" do
    test "renders dashboard", %{conn: conn} do
      {:ok, _dashboard_live, html} = live(conn, "/dashboard_new")
      assert html =~ "~/logs"
    end
  end
end
