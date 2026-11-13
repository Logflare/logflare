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
      conn = previous_session(conn, @previous_session, user)

      expected_team_id =
        if @previous_session == :same_user, do: get_session(conn, :last_switched_team_id)

      conn =
        conn
        |> pending_integration(@integration)
        |> google_callback(user)

      assert get_session(conn, :current_email) == user.email
      assert get_session(conn, :last_switched_team_id) == expected_team_id
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

  for {scenario, extra_params} <- [
        {"with state", %{"state" => "state&with=reserved\"characters"}},
        {"without state", %{}}
      ] do
    @extra_params extra_params

    test "OAuth authorization survives a login detour #{scenario}", %{conn: conn, user: user} do
      params = Map.merge(oauth_params(), @extra_params)
      conn = get(conn, "/oauth/authorize", params)

      assert redirected_to(conn) == "/auth/login"
      assert get_session(conn, :oauth_params) == params

      conn =
        build_conn()
        |> Plug.Test.init_test_session(get_session(conn))
        |> google_callback(user)

      path = redirected_to(conn)
      uri = URI.parse(path)

      assert uri.path == "/oauth/authorize"
      assert URI.decode_query(uri.query) == params
      assert get_session(conn, :current_email) == user.email
      refute get_session(conn, :oauth_params)

      conn =
        build_conn()
        |> Plug.Test.init_test_session(get_session(conn))
        |> get(path)

      document = conn |> html_response(200) |> Floki.parse_document!()

      forms = Floki.find(document, "#authorize-container form")

      for form <- forms do
        assert Floki.attribute(form, "input[name=state]", "value") == List.wrap(params["state"])
      end

      form =
        Enum.find(forms, fn form -> Floki.find(form, "button") |> Floki.text() == "Authorize" end)

      form_params =
        form
        |> Floki.find("input")
        |> Map.new(fn input ->
          {hd(Floki.attribute(input, "name")), List.first(Floki.attribute(input, "value"), "")}
        end)

      conn = conn |> recycle() |> post("/oauth/authorize", form_params)
      callback = conn |> redirected_to() |> URI.parse()
      query = URI.decode_query(callback.query)

      assert callback.host == "example.com"
      assert query["code"]
      assert Map.get(query, "state", "") == Map.get(params, "state", "")
    end
  end

  defp previous_session(conn, :none, _user), do: conn

  defp previous_session(conn, :same_user, user) do
    team = insert(:team, user: user)

    conn
    |> login_user(user)
    |> put_session(:last_switched_team_id, team.id)
  end

  defp previous_session(conn, :other_user, _user) do
    previous_user = insert(:user)
    previous_session(conn, :same_user, previous_user)
  end

  defp pending_integration(conn, :oauth), do: put_session(conn, :oauth_params, oauth_params())

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

  @spec oauth_params() :: map()
  defp oauth_params do
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

    %{
      "client_id" => application.uid,
      "redirect_uri" => application.redirect_uri,
      "scope" => "read write",
      "response_type" => "code"
    }
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
