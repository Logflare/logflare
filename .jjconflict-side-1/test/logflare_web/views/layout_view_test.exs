defmodule LogflareWeb.LayoutViewTest do
  use LogflareWeb.ConnCase, async: true
  alias LogflareWeb.LayoutView
  @endpoint LogflareWeb.Endpoint

  import Phoenix.View

  setup do
    conn =
      build_conn(:get, "/anything")
      |> Plug.Conn.put_private(:phoenix_endpoint, @endpoint)

    {:ok, conn: conn}
  end

  test "renders layout root correctly", %{conn: conn} do
    assert render_to_string(LayoutView, "root.html", conn: conn, inner_content: "some-value") =~
             "some-value"
  end
end
