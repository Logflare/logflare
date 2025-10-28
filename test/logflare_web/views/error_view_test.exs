defmodule LogflareWeb.ErrorViewTest do
  use LogflareWeb.ConnCase, async: true
  alias LogflareWeb.ErrorView
  @endpoint LogflareWeb.Endpoint

  import Phoenix.View

  setup do
    conn =
      build_conn(:get, "/anything")
      |> Plug.Conn.put_private(:phoenix_endpoint, @endpoint)

    {:ok, conn: conn}
  end

  test "renders 404.html", %{conn: conn} do
    assert render_to_string(ErrorView, "404_page.html", conn: conn) =~ "404"
    assert render_to_string(ErrorView, "404_page.html", conn: conn) =~ "Page not found"
  end

  test "renders 401.html", %{conn: conn} do
    assert render_to_string(ErrorView, "401_page.html", conn: conn) =~ "401"
    assert render_to_string(ErrorView, "401_page.html", conn: conn) =~ "Unauthorized"
  end

  test "renders 403.html", %{conn: conn} do
    assert render_to_string(ErrorView, "403_page.html", conn: conn) =~ "403"
    assert render_to_string(ErrorView, "403_page.html", conn: conn) =~ "Forbidden"
  end

  test "renders 500.html", %{conn: conn} do
    assert render_to_string(ErrorView, "500_page.html", conn: conn) =~ "500"
    assert render_to_string(ErrorView, "500_page.html", conn: conn) =~ "Server error"
  end
end
