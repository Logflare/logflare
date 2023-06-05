defmodule LogflareWeb.OAuthControllerTests do
  @moduledoc false
  use LogflareWeb.ConnCase

  test "unrecognized provider handling", %{conn: conn} do
    for path <- ["/auth/something", "/auth/something/callback"] do
      conn =
        conn
        |> get(path, %{})

      assert html_response(conn, 302)
      assert conn.assigns.flash["error"] =~ "Authentication error"
    end
  end

  test "oauth2 callback redirect work correctly post-sign in", %{conn: conn} do
    insert(:plan)
    user = insert(:user)
    conn = login_user(conn, user)
    env_oauth_config = Application.get_env(:logflare, ExOauth2Provider)

    # create the oauth2 applications
    admin = insert(:user)

    {:ok, application} =
      ExOauth2Provider.Applications.create_application(
        admin,
        %{
          name: "Cloudflare App",
          redirect_uri: "https://www.cloudflare.com/apps/oauth/",
          scope: "read write",
          secret: "",
          uid: "d8a8af24ada66bfa29477d746d74ae7a8833d80019c6fa285f07fe6159491a5f"
        },
        env_oauth_config
      )

    search =
      URI.encode_query(%{
        scope: "read write",
        response_type: "code",
        redirect_uri: application.redirect_uri,
        client_id: application.uid,
        # additional app metadata should be forwarded
        "user.email": "test@logflare.app",
        "site.name": "test.logflare.app"
      })

    uri = "/oauth/authorize?#{search}"
    conn = get(conn, uri, %{})
    assert html_response(conn, 302)
    assert conn.assigns.flash["error"] == nil
    assert redirected_to(conn) =~ "www.cloudflare.com"
  end
end
