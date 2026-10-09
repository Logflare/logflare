defmodule LogflareWeb.IntegrationSigninTest do
  use LogflareWeb.ConnCase

  alias ExOauth2Provider.Applications
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias LogflareWeb.Auth.OauthController

  setup do
    insert(:plan)
    user = insert(:user, provider: "google", provider_uid: "google-integration-user")

    stub(BigQueryAdaptor, :update_iam_policy, fn _user -> :ok end)
    stub(BigQueryAdaptor, :patch_dataset_access, fn _user -> {:ok, :patch_attempted} end)

    config = Application.get_env(:logflare, Logflare.Vercel.Client)

    Application.put_env(
      :logflare,
      Logflare.Vercel.Client,
      Keyword.put(config, :install_vercel_uri, "https://vercel.com/integrations/test")
    )

    on_exit(fn -> Application.put_env(:logflare, Logflare.Vercel.Client, config) end)

    {:ok, user: user}
  end

  for integration <- [:oauth, :vercel], previous_session <- [:none, :same_user, :other_user] do
    @integration integration
    @previous_session previous_session

    test "Google sign-in completes #{@integration} with #{@previous_session} prior session", %{
      conn: conn,
      user: user
    } do
      conn =
        conn
        |> previous_session(@previous_session, user)
        |> pending_integration(@integration)
        |> google_callback(user)

      assert get_session(conn, :current_email) == user.email
      refute get_session(conn, :oauth_params)
      refute get_session(conn, :vercel_setup)

      path =
        case @integration do
          :oauth ->
            assert redirected_to(conn) =~ "/oauth/authorize?"
            redirected_to(conn)

          :vercel ->
            assert redirected_to(conn) == "http://www.example.com/integrations/vercel/edit"
            "/integrations/vercel/edit"
        end

      conn =
        build_conn()
        |> Plug.Test.init_test_session(get_session(conn))
        |> get(path)

      assert html_response(conn, 200)
      assert conn.assigns.user.id == user.id
    end
  end

  defp previous_session(conn, :none, _user), do: conn
  defp previous_session(conn, :same_user, user), do: login_user(conn, user)
  defp previous_session(conn, :other_user, _user), do: login_user(conn, insert(:user))

  defp pending_integration(conn, :oauth) do
    {:ok, application} =
      Applications.create_application(
        insert(:user),
        %{
          name: "Integration test app",
          scopes: "read write",
          redirect_uri: "https://example.com/callback"
        },
        Application.get_env(:logflare, ExOauth2Provider)
      )

    put_session(conn, :oauth_params, %{
      "client_id" => application.uid,
      "redirect_uri" => application.redirect_uri,
      "scope" => "read write",
      "response_type" => "code"
    })
  end

  defp pending_integration(conn, :vercel) do
    put_session(conn, :vercel_setup, %{
      "auth_params" => %{
        "installation_id" => "install_#{System.unique_integer([:positive])}",
        "access_token" => "test-vercel-token",
        "token_type" => "Bearer",
        "vercel_user_id" => "test-vercel-user"
      },
      "next" => "http://www.example.com/integrations/vercel/edit"
    })
  end

  defp google_callback(conn, user) do
    auth = %Ueberauth.Auth{
      uid: user.provider_uid,
      provider: :google,
      info: %Ueberauth.Auth.Info{email: user.email, name: user.name, image: user.image},
      credentials: %Ueberauth.Auth.Credentials{token: "test-google-token"}
    }

    %{conn | secret_key_base: LogflareWeb.Endpoint.config(:secret_key_base)}
    |> assign(:flash, %{})
    |> assign(:ueberauth_auth, auth)
    |> OauthController.callback(%{"provider" => "google"})
  end
end
