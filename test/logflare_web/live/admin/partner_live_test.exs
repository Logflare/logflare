defmodule LogflareWeb.Admin.PartnerLiveTest do
  use LogflareWeb.ConnCase

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

    refute html =~ "No partners created yet"

    Enum.each(partners, fn %{name: name, token: token} ->
      assert html =~ name
      assert html =~ token
    end)
  end

  test "creates new partner on form submit", %{conn: conn} do
    {:ok, view, _html} = live(conn, "/admin/partner")

    name = TestUtils.random_string()

    html =
      view
      |> form("#create-partner", %{"partner" => %{"name" => name}})
      |> render_submit()

    assert html =~ name
  end

  test "deletes partner on delete button click", %{conn: conn} do
    [%{token: token, name: name} | _others] = insert_list(5, :partner)

    {:ok, view, html} = live(conn, "/admin/partner")

    assert html =~ name

    html =
      view
      |> element("#delete-#{token}")
      |> render_click()

    refute html =~ name
  end

  test "creates token on create token button click", %{conn: conn} do
    %{token: token} = partner = insert(:partner)
    description = "My Token for partner #{partner.name}"

    {:ok, view, _html} = live(conn, "/admin/partner")

    _html =
      view
      |> form("#generate-#{token}", %{"description" => description})
      |> render_submit()

    [%{token: access_token}] = Logflare.Auth.list_valid_access_tokens(partner)

    assert has_element?(view, "#created_token", access_token)
    assert has_element?(view, "#description", description)

    _html =
      view
      |> element("#dismiss-created-token")
      |> render_click()

    refute has_element?(view, "#created_token", access_token)
    refute has_element?(view, "#description", description)
  end

  test "created token disappears on refresh", %{conn: conn} do
    %{token: token} = partner = insert(:partner)

    {:ok, view, _html} = live(conn, "/admin/partner")

    _html =
      view
      |> form("#generate-#{token}")
      |> render_submit()

    [%{token: access_token}] = Logflare.Auth.list_valid_access_tokens(partner)

    assert has_element?(view, "#created_token", access_token)

    {:ok, view, _html} = live(conn, "/admin/partner")

    refute has_element?(view, "#created_token", access_token)
  end
end
