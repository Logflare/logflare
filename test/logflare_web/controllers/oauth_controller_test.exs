defmodule LogflareWeb.OAuthControllerTest do
  use LogflareWeb.ConnCase

  alias LogflareWeb.Auth.OauthController

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

  test "slack oauth2 for adding alert slack hooks", %{conn: conn} do
    insert(:plan)
    user = insert(:user)
    alert_query = insert(:alert, user: user)

    conn =
      conn
      |> login_user(user)
      |> bypass_through(LogflareWeb.Router, [:browser])
      |> assign(:ueberauth_auth, %{
        extra: %{
          raw_info: %{
            token: %{other_params: %{"incoming_webhook" => %{"url" => "https://some-url.com"}}}
          }
        }
      })
      |> get(~p"/auth/slack/callback")
      |> OauthController.callback(%{
        "code" =>
          "893134481777.5938142341456.c62b0cecf9de9bc062290c92267dd94ac6cd99f81f53bf8b0da7ef6d31994077",
        "provider" => "slack",
        "state" =>
          Jason.encode!(%{
            action: "save_hook_url",
            alert_query_id: alert_query.id
          })
      })

    assert html_response(conn, 302)
    assert conn.assigns.flash["info"] =~ "Alert connected to Slack!"
    assert redirected_to(conn) == ~p"/alerts/#{alert_query.id}"
  end

  describe "bug: ueberauth port does not match url config" do
    setup do
      start_supervised!(Logflare.SystemMetricsSup)
      config = Application.get_env(:logflare, LogflareWeb.Endpoint)

      on_exit(fn ->
        Application.put_env(:logflare, LogflareWeb.Endpoint, config)
      end)

      {:ok, config: config}
    end

    test "navbar OAuth2 link should exclude port if host is provided", %{
      conn: conn,
      config: config
    } do
      updated = Keyword.put(config, :url, host: "www.something.com", port: 3232)
      Application.put_env(:logflare, LogflareWeb.Endpoint, updated)
      conn = conn |> get(Routes.oauth_path(conn, :request, "google"))
      assert redirected_to(conn, 302) =~ "3232"
    end

    test "navbar OAuth2 link should exclude port if host is provided with string port", %{
      conn: conn,
      config: config
    } do
      updated = Keyword.put(config, :url, host: "www.something.com", port: "3232")
      Application.put_env(:logflare, LogflareWeb.Endpoint, updated)
      conn = conn |> get(Routes.oauth_path(conn, :request, "google"))
      assert redirected_to(conn, 302) =~ "3232"
    end
  end
end
