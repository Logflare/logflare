defmodule LogflareWeb.OAuthControllerTests do
  @moduledoc false
  use LogflareWeb.ConnCase

  test "unrecognized provider handling", %{conn: conn} do
    for path <- ["/auth/something", "/auth/something/callback"] do
      conn =
        conn
        |> get(path, %{})

      assert html_response(conn, 302)
      assert get_flash(conn)["error"] =~ "Authentication error"
    end
  end
end
