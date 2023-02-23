defmodule LogflareWeb.Admin.PartnerLiveTest do
  use LogflareWeb.ConnCase
  import Phoenix.ConnTest
  import Phoenix.LiveViewTest
  @endpoint LogflareWeb.Endpoint

  describe "PartnerLive" do
    setup %{conn: conn} do
      user = insert(:user, admin: true)
      insert(:plan)
      conn = login_user(conn, user)
      {:ok, conn: conn}
    end

    test "view with no partners renders", %{conn: conn} do
      {:ok, _view, html} = live(conn, "/admin/partner")

      assert html =~ "No partners created yet"
    end

    test "view with partners renders", %{conn: conn} do
      partners = insert_list(5, :partner)
      {:ok, _view, html} = live(conn, "/admin/partner")

      Enum.each(partners, fn %{name: name, token: token, auth_token: auth_token} ->
        assert html =~ name
        assert html =~ token
        assert html =~ auth_token
      end)
    end

    test "creates new partner on form submit", %{conn: conn} do
      {:ok, view, _html} = live(conn, "/admin/partner")

      name = TestUtils.random_string()

      html =
        view
        |> element("form")
        |> render_submit(%{"partner" => %{"name" => name}})

      assert html =~ name
    end

    test "deletes partner on delete button click", %{conn: conn} do
      [%{token: token} | _others] = insert_list(5, :partner)

      {:ok, view, _html} = live(conn, "/admin/partner")

      name = TestUtils.random_string()

      html =
        view
        |> element("#delete-#{token}")
        |> render_click()

      refute html =~ name
    end
  end
end
