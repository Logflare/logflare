defmodule LogflareWeb.OauthProviderTemplatesTest do
  use LogflareWeb.ConnCase

  alias ExOauth2Provider.Applications
  alias Logflare.OauthAccessGrants.OauthAccessGrant
  alias Logflare.Repo

  setup %{conn: conn} do
    insert(:plan)
    user = insert(:user)

    {:ok, conn: login_user(conn, user), user: user}
  end

  test "authorized applications renders an empty list", %{conn: conn} do
    body = conn |> get("/oauth/authorized_applications") |> html_response(200)

    assert body =~ "Authorized Applications"
    assert body |> Floki.parse_document!() |> Floki.find("tbody tr") == []
  end

  test "authorized applications lists only the user's apps and supports revocation", %{
    conn: conn,
    user: user
  } do
    application = create_application()
    other_application = create_application(%{name: "Other user's app"})

    token =
      insert(:access_token, resource_owner: user, application: application, scopes: "read write")

    insert(:access_token, application: other_application, scopes: "read write")

    conn = get(conn, "/oauth/authorized_applications")
    body = html_response(conn, 200)
    document = Floki.parse_document!(body)
    revoke_path = Routes.oauth_authorized_application_path(conn, :delete, application)

    assert Floki.find(document, "tbody tr") |> length() == 1
    assert body =~ application.name
    refute body =~ other_application.name
    assert Floki.attribute(document, "tbody a", "data-to") == [revoke_path]
    assert Floki.attribute(document, "tbody a", "data-method") == ["delete"]
    assert Floki.attribute(document, "tbody a", "data-csrf") != []

    conn = conn |> recycle() |> delete(revoke_path)
    assert redirected_to(conn) == "/oauth/authorized_applications"
    assert Repo.reload!(token).revoked_at

    body = conn |> recycle() |> get("/oauth/authorized_applications") |> html_response(200)
    assert body |> Floki.parse_document!() |> Floki.find("tbody tr") == []
  end

  test "fresh authorization renders scopes and forms that preserve OAuth parameters", %{
    conn: conn
  } do
    application = create_application(%{name: "Example <script>client</script>"})
    params = authorization_params(application)
    body = conn |> get("/oauth/authorize", params) |> html_response(200)
    document = Floki.parse_document!(body)

    assert body =~ "Example &lt;script&gt;client&lt;/script&gt;"
    assert Floki.find(document, "#authorize-container script") == []

    assert Floki.find(document, ".oauth-permissions li") |> Enum.map(&Floki.text/1) == [
             "read",
             "write"
           ]

    forms = Floki.find(document, "#authorize-container form")
    assert length(forms) == 2

    for form <- forms do
      assert Floki.attribute([form], "form", "action") == ["/oauth/authorize"]
      assert Floki.attribute([form], "form", "method") == ["post"]
      assert form_params(form)["_csrf_token"] != nil
      assert Map.take(form_params(form), Map.keys(params)) == params
    end

    assert form_params(consent_form(body, "Deny"))["_method"] == "delete"
    refute Map.has_key?(form_params(consent_form(body, "Authorize")), "_method")
  end

  test "submitting the authorize form issues a code and preserves state", %{
    conn: conn,
    user: user
  } do
    application = create_application()
    conn = get(conn, "/oauth/authorize", authorization_params(application))
    params = conn |> html_response(200) |> consent_form("Authorize") |> form_params()

    conn = conn |> recycle() |> post("/oauth/authorize", params)
    uri = conn |> redirected_to() |> URI.parse()
    query = URI.decode_query(uri.query)

    assert uri.host == "example.com"
    assert query["state"] == params["state"]
    assert query["code"]
    refute query["error"]
    assert Repo.get_by!(OauthAccessGrant, token: query["code"]).resource_owner_id == user.id
  end

  test "submitting the deny form returns access denied and preserves state", %{conn: conn} do
    application = create_application()
    conn = get(conn, "/oauth/authorize", authorization_params(application))
    params = conn |> html_response(200) |> consent_form("Deny") |> form_params()

    conn = conn |> recycle() |> delete("/oauth/authorize", params)
    uri = conn |> redirected_to() |> URI.parse()
    query = URI.decode_query(uri.query)

    assert uri.host == "example.com"
    assert query["state"] == params["state"]
    assert query["error"] == "access_denied"
    refute query["code"]
  end

  test "an invalid client renders the OAuth error instead of crashing", %{conn: conn} do
    body =
      conn
      |> get("/oauth/authorize", %{client_id: "unknown-client", response_type: "code"})
      |> html_response(422)

    assert body =~ "An error has occurred"
    assert body =~ "Client authentication failed"
  end

  test "native authorization redirects to the code display page", %{conn: conn, user: user} do
    application = create_application(%{redirect_uri: "urn:ietf:wg:oauth:2.0:oob"})
    insert(:access_token, resource_owner: user, application: application, scopes: "read write")

    conn = get(conn, "/oauth/authorize", authorization_params(application))
    path = redirected_to(conn)
    assert path =~ "/oauth/authorize/"
    code = path |> String.split("/") |> List.last()

    body = conn |> recycle() |> get(path) |> html_response(200)

    assert body |> Floki.parse_document!() |> Floki.find("#authorization_code") |> Floki.text() ==
             code
  end

  test "OAuth pages still require authentication", %{conn: conn} do
    for path <- ["/oauth/authorized_applications", "/oauth/authorize"] do
      conn = conn |> clear_session() |> get(path)
      assert redirected_to(conn) == "/auth/login"
    end
  end

  defp create_application(attrs \\ %{}) do
    config = Application.get_env(:logflare, ExOauth2Provider)

    {:ok, application} =
      Applications.create_application(
        insert(:user),
        Map.merge(
          %{
            name: "Example OAuth app",
            redirect_uri: "https://example.com/callback",
            scopes: "read write"
          },
          attrs
        ),
        config
      )

    application
  end

  defp authorization_params(application) do
    %{
      "client_id" => application.uid,
      "redirect_uri" => application.redirect_uri,
      "response_type" => "code",
      "scope" => "read write",
      "state" => "state&with=reserved\"characters"
    }
  end

  defp consent_form(body, label) do
    body
    |> Floki.parse_document!()
    |> Floki.find("#authorize-container form")
    |> Enum.find(fn form -> Floki.find([form], "button") |> Floki.text() == label end)
  end

  defp form_params(form) do
    form
    |> List.wrap()
    |> Floki.find("input")
    |> Map.new(fn input ->
      [name] = Floki.attribute([input], "input", "name")
      [value] = Floki.attribute([input], "input", "value")
      {name, value}
    end)
  end
end
