defmodule LogflareWeb.Plugs.DocsAssetsTest do
  use LogflareWeb.ConnCase, async: true

  test "serves png images", %{conn: conn} do
    conn = get(conn, "/docs/assets/concepts/lql/lql-reference.png")

    assert conn.status == 200
    assert get_resp_header(conn, "content-type") == ["image/png"]
  end

  test "serves gif images", %{conn: conn} do
    conn = get(conn, "/docs/assets/integrations/vercel/example-vercel-search.gif")

    assert conn.status == 200
    assert get_resp_header(conn, "content-type") == ["image/gif"]
  end

  test "does not serve markdown files", %{conn: conn} do
    conn = get(conn, "/docs/assets/concepts/lql/index.md")

    assert conn.status == 404
  end

  test "does not serve docs metadata files", %{conn: conn} do
    conn = get(conn, "/docs/assets/concepts/_category_.json")

    assert conn.status == 404
  end

  test "does not serve files outside the docs sections", %{conn: conn} do
    conn = get(conn, "/docs/assets/../mix.exs")

    assert conn.status == 404
  end
end
