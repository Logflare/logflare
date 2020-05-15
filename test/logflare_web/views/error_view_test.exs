defmodule LogflareWeb.ErrorViewTest do
  use LogflareWeb.ConnCase, async: true
  alias LogflareWeb.ErrorView

  import Phoenix.View

  test "renders 404.html", %{conn: conn} do
    assert render_to_string(ErrorView, "404_page.html", conn: conn) =~ "404"
    assert render_to_string(ErrorView, "404_page.html", conn: conn) =~ "not found"
  end

  test "renders 401.html", %{conn: conn} do
    assert render_to_string(ErrorView, "404_page.html", conn: conn) =~ "404"
    assert render_to_string(ErrorView, "404_page.html", conn: conn) =~ "not found"
  end
end
