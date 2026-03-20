defmodule LogflareWeb.Plugs.VerifyApiAccessTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  alias LogflareWeb.Plugs.VerifyApiAccess

  setup do
    insert(:plan)
    user = insert(:user)
    endpoint_auth = insert(:endpoint, user: user, enable_auth: true)
    endpoint_open = insert(:endpoint, user: user, enable_auth: false)
    source = insert(:source, user: user)
    {:ok, access_token} = Logflare.Auth.create_access_token(user)

    [
      user: user,
      endpoint_auth: endpoint_auth,
      endpoint_open: endpoint_open,
      source: source,
      access_token: access_token
    ]
  end

  describe "partner impersonation" do
    setup do
      conn = build_conn(:post, "/api/sources", %{"name" => "some name"})

      {:ok, conn: conn}
    end

    test "can impersonate partner-provisioned user", %{conn: conn} do
      partner = insert(:partner)
      user = insert(:user, partner: partner)

      conn
      |> put_req_header("x-lf-partner-user", user.token)
      |> add_partner_access_token(partner)
      |> VerifyApiAccess.call(%{})
      |> assert_authorized(user)

      partner = insert(:partner)

      conn
      |> put_req_header("x-lf-partner-user", user.token)
      |> add_partner_access_token(partner)
      |> VerifyApiAccess.call(%{})
      |> assert_unauthorized()
    end
  end

  describe "source ingestion auth" do
    setup %{source: source} do
      conn =
        build_conn(:post, "/logs", %{"source" => Atom.to_string(source.token)})
        |> assign(:source, source)

      {:ok, conn: conn}
    end

    test "x-api-key verifies correctly", %{conn: conn, user: user, access_token: access_token} do
      conn
      |> put_req_header("x-api-key", access_token.token)
      |> VerifyApiAccess.call(%{})
      |> assert_authorized(user)

      conn
      |> put_req_header("x-api-key", user.api_key)
      |> VerifyApiAccess.call(%{})
      |> assert_authorized(user)
    end

    test "Authorization header verifies correctly", %{conn: conn, user: user, access_token: token} do
      conn
      |> put_req_header("authorization", "Bearer #{token.token}")
      |> VerifyApiAccess.call(%{})
      |> assert_authorized(user)

      conn
      |> put_req_header("authorization", "Bearer #{user.api_key}")
      |> VerifyApiAccess.call(%{})
      |> assert_authorized(user)
    end

    test "query params verifies correctly", %{conn: conn, user: user, access_token: access_token} do
      new_params = Map.merge(conn.params, %{"api_key" => access_token.token})

      conn
      |> Map.put(:params, new_params)
      |> VerifyApiAccess.call(%{})
      |> assert_authorized(user)

      new_params = Map.merge(conn.params, %{"api_key" => user.api_key})

      conn
      |> Map.put(:params, new_params)
      |> VerifyApiAccess.call(%{})
      |> assert_authorized(user)
    end

    test "halts request with bad api key", %{conn: conn} do
      conn
      |> put_req_header("x-api-key", "123")
      |> VerifyApiAccess.call(%{})
      |> assert_unauthorized()
    end
  end

  describe "endpoint.enable_auth=true" do
    setup %{endpoint_auth: endpoint} do
      conn =
        build_conn(:post, "/endpoints/query/#{endpoint.token}", %{"token" => endpoint.token})
        |> assign(:endpoint, endpoint)

      {:ok, conn: conn}
    end

    test "x-api-key verifies correctly", %{conn: conn, user: user, access_token: access_token} do
      conn
      |> put_req_header("x-api-key", access_token.token)
      |> VerifyApiAccess.call(%{})
      |> assert_authorized(user)

      conn
      |> put_req_header("x-api-key", user.api_key)
      |> VerifyApiAccess.call(%{})
      |> assert_authorized(user)
    end

    test "Authorization header verifies correctly", %{conn: conn, user: user, access_token: token} do
      conn
      |> put_req_header("authorization", "Bearer #{token.token}")
      |> VerifyApiAccess.call(%{})
      |> assert_authorized(user)

      conn
      |> put_req_header("authorization", "Bearer #{user.api_key}")
      |> VerifyApiAccess.call(%{})
      |> assert_authorized(user)
    end

    test "query params verifies correctly", %{conn: conn, user: user, access_token: access_token} do
      new_params = Map.merge(conn.params, %{"api_key" => access_token.token})

      conn
      |> Map.put(:params, new_params)
      |> VerifyApiAccess.call(%{})
      |> assert_authorized(user)

      new_params = Map.merge(conn.params, %{"api_key" => user.api_key})

      conn
      |> Map.put(:params, new_params)
      |> VerifyApiAccess.call(%{})
      |> assert_authorized(user)
    end

    test "halts request with bad api key", %{conn: conn} do
      conn
      |> put_req_header("x-api-key", "123")
      |> VerifyApiAccess.call(%{})
      |> assert_unauthorized()
    end
  end

  describe "endpoint.enable_auth=false" do
    setup %{endpoint_open: endpoint} do
      conn =
        build_conn(:get, "/endpoints/query/#{endpoint.token}", %{"token" => endpoint.token})
        |> assign(:resource_type, :endpoint)

      {:ok, conn: conn}
    end

    test "does not halt request if resource type is specified", %{conn: conn} do
      conn = VerifyApiAccess.call(conn, %{})
      assert conn.halted == false
      assert Map.get(conn.assigns, :user) == nil
    end
  end

  test "private scope", %{user: user} do
    {:ok, access_token} = Logflare.Auth.create_access_token(user, %{scopes: "private"})

    build_conn(:get, "/any", %{})
    |> put_req_header("x-api-key", access_token.token)
    |> VerifyApiAccess.call(%{scopes: ~w(private)})
    |> assert_authorized(user)

    build_conn(:get, "/any", %{})
    |> put_req_header("x-api-key", access_token.token)
    |> VerifyApiAccess.call(%{scopes: ~w(private)})
    |> assert_authorized(user)

    # no scope set
    {:ok, access_token_empty} = Logflare.Auth.create_access_token(user)

    build_conn(:get, "/any", %{})
    |> put_req_header("x-api-key", access_token_empty.token)
    |> VerifyApiAccess.call(%{scopes: ~w(private)})
    |> assert_unauthorized()

    # user.api_key
    build_conn(:get, "/any", %{})
    |> put_req_header("x-api-key", user.api_key)
    |> VerifyApiAccess.call(%{scopes: ~w(private)})
    |> assert_unauthorized()
  end

  defp assert_authorized(conn, user) do
    assert conn.halted == false
    assert conn.assigns.user.id == user.id
  end

  defp assert_unauthorized(conn) do
    assert conn.halted == true
    assert conn.resp_body |> inspect() |> String.downcase() =~ "error"
    assert conn.resp_body |> inspect() |> String.downcase() =~ "unauthorized"
    assert conn.status == 401
  end
end
