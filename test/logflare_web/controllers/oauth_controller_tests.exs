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

  test "oauth2 callback redirect work correctly post-sign in", %{conn: conn} do
    insert(:plan)
    user = insert(:user)
    conn = login_user(conn, user)

    search =
      URI.encode_query(%{
        scope: "read write",
        response_type: "code",
        redirect_uri: "www.cloudflare.com/apps/oauth/",
        client_id: "my-client-id",
        # additional app metadata should be forwarded
        "user.email": "test@logflare.app",
        "site.name": "test.logflare.app"
      })

    uri = "/oauth/authorize?#{search}"
    conn = get(conn, uri, %{})
    assert html_response(conn, 302)
    assert get_flash(conn)["error"] == nil
    assert redirected_to(conn) =~ "www.cloudflare.com"
  end
end
