defmodule LogflareWeb.Admin.ClusterLiveTest do
  use LogflareWeb.ConnCase

  setup do
    insert(:plan)

    :ok
  end

  describe "when logged in as admin" do
    setup %{conn: conn} do
      admin = insert(:user, admin: true)
      {:ok, conn: login_user(conn, admin), user: admin}
    end

    test "successfully renders the current node", %{conn: conn} do
      assert {:ok, _view, html} =
               conn
               |> live(~p"/admin/cluster")

      assert html =~ "Updates every 2 seconds."
      assert html =~ "#{Node.self()}"
    end

    test "can shutdown current node", %{conn: conn} do
      expect(Logflare.Admin, :shutdown, fn node ->
        assert node == Node.self()
        {:ok, nil}
      end)

      assert {:ok, view, html} =
               conn
               |> live(~p"/admin/cluster")

      assert html =~ "Updates every 2 seconds."
      assert html =~ "#{Node.self()}"

      view
      |> element("#shutdown-self")
      |> render_click()
    end

    test "shutdown task messages are correctly handled", %{conn: conn} do
      node_name = :node@example

      expect(Logflare.Admin, :shutdown, fn node ->
        assert node == node_name

        Task.async(fn ->
          Process.sleep(100)
        end)

        {:ok, nil}
      end)

      assert {:ok, view, _html} =
               conn
               |> live(~p"/admin/cluster")

      assert render_click(view, "shutdown", %{"node" => to_string(node_name)}) =~
               "Node shutdown initiated for #{node_name}"

      Process.sleep(200)

      assert render(view) =~ "Node shutdown initiated for #{node_name}"
    end
  end

  test "returns 403 when user is not admin", %{conn: conn} do
    assert conn
           |> login_user(insert(:user, admin: false))
           |> get(~p"/admin/cluster")
           |> html_response(403) =~ "Forbidden"
  end

  test "redirects to login when not logged in", %{conn: conn} do
    assert conn
           |> get(~p"/admin/cluster")
           |> redirected_to() == ~p"/auth/login"
  end
end
