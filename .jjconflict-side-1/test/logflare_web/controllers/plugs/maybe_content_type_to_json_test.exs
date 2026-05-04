defmodule LogflareWeb.Plugs.MaybeContentTypeToJsonTest do
  use LogflareWeb.ConnCase, async: true

  alias LogflareWeb.Plugs.MaybeContentTypeToJson

  test "rewrites application/csp-report to application/json", %{conn: conn} do
    conn =
      conn
      |> put_req_header("content-type", "application/csp-report")
      |> MaybeContentTypeToJson.call([])

    assert get_req_header(conn, "content-type") == ["application/json"]
  end

  test "does not modify other content types", %{conn: conn} do
    conn =
      conn
      |> put_req_header("content-type", "text/plain")
      |> MaybeContentTypeToJson.call([])

    assert get_req_header(conn, "content-type") == ["text/plain"]
  end

  test "does not modify conn when there's no content-type header", %{conn: conn} do
    conn = MaybeContentTypeToJson.call(conn, [])

    assert get_req_header(conn, "content-type") == []
  end
end
