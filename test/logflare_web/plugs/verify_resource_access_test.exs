defmodule LogflareWeb.Plugs.VerifyResourceAccessTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  alias LogflareWeb.Plugs.VerifyResourceAccess

  setup do
    user = insert(:user)
    endpoint = insert(:endpoint, user: user, enable_auth: true)
    source = insert(:source, user: user)
    {:ok, user: user, source: source, endpoint: endpoint}
  end

  describe "source" do
    setup %{source: source, user: user} do
      conn =
        build_conn(:post, "/logs", %{"source" => Atom.to_string(source.token)})
        |> assign(:user, user)
        |> assign(:source, source)
        |> assign(:resource_type, :source)

      [conn: conn]
    end

    test "valid - ingest into any", %{conn: initial, user: user} do
      for scopes <- [
            "",
            "public",
            "ingest"
          ] do
        {:ok, access_token} = Logflare.Auth.create_access_token(user, %{scopes: scopes})

        conn =
          initial
          |> assign(:access_token, access_token)
          |> VerifyResourceAccess.call(%{})

        refute conn.halted
      end
    end

    test "valid - ingest into one source", %{conn: conn, source: source, user: user} do
      {:ok, access_token} =
        Logflare.Auth.create_access_token(user, %{scopes: "ingest:source:#{source.id}"})

      conn =
        conn
        |> assign(:access_token, access_token)
        |> VerifyResourceAccess.call(%{})

      refute conn.halted
    end

    test "invalid - no access token", %{conn: initial_conn, source: source, user: user} do
      # no access token
      conn =
        initial_conn
        |> assign(:user, insert(:user))
        |> VerifyResourceAccess.call(%{})

      assert conn.halted
      assert conn.status == 401

      # invalid scope check
      {:ok, access_token} =
        Logflare.Auth.create_access_token(user, %{scopes: "ingest:source:#{source.id + 4}"})

      conn =
        initial_conn
        |> assign(:access_token, access_token)
        |> VerifyResourceAccess.call(%{})

      assert conn.halted
      assert conn.status == 401
    end

    test "invalid - specific source", %{conn: initial_conn, source: source, user: user} do
      {:ok, access_token} =
        Logflare.Auth.create_access_token(user, %{scopes: "ingest:source:#{source.id + 4}"})

      conn =
        initial_conn
        |> assign(:access_token, access_token)
        |> VerifyResourceAccess.call(%{})

      assert conn.halted and conn.status == 401
    end
  end

  describe "endpoints" do
    setup %{endpoint: endpoint, user: user} do
      conn =
        build_conn(:get, "/endpoints/query/#{endpoint.token}", %{"token" => endpoint.token})
        |> assign(:user, user)
        |> assign(:resource_type, :endpoint)
        |> assign(:endpoint, endpoint)

      [conn: conn]
    end

    test "valid - query one", %{conn: conn, user: user, endpoint: endpoint} do
      {:ok, access_token} =
        Logflare.Auth.create_access_token(user, %{scopes: "query:endpoint:#{endpoint.id}"})

      conn =
        conn
        |> assign(:access_token, access_token)
        |> VerifyResourceAccess.call(%{})

      refute conn.halted
    end

    test "valid - query any", %{conn: conn, user: user} do
      {:ok, access_token} = Logflare.Auth.create_access_token(user, %{scopes: "query"})

      conn =
        conn
        |> assign(:access_token, access_token)
        |> VerifyResourceAccess.call(%{})

      refute conn.halted
    end

    test "invalid - wrong scope action", %{conn: conn, user: user} do
      {:ok, access_token} = Logflare.Auth.create_access_token(user, %{scopes: "ingest"})

      conn =
        conn
        |> assign(:access_token, access_token)
        |> VerifyResourceAccess.call(%{})

      assert conn.halted and conn.status == 401
    end

    test "invalid - wrong resource scope", %{conn: conn, user: user, endpoint: endpoint} do
      {:ok, access_token} =
        Logflare.Auth.create_access_token(user, %{scopes: "query:endpoint:#{endpoint.id + 4}"})

      conn =
        conn
        |> assign(:access_token, access_token)
        |> VerifyResourceAccess.call(%{})

      assert conn.halted and conn.status == 401
    end

    test "invalid - no scope", %{conn: conn, user: user} do
      {:ok, access_token} = Logflare.Auth.create_access_token(user, %{scopes: ""})

      conn =
        conn
        |> assign(:access_token, access_token)
        |> VerifyResourceAccess.call(%{})

      assert conn.halted and conn.status == 401
    end
  end

  test "no resource provided", %{user: user} do
    conn =
      build_conn(:get, "/any", %{})
      |> assign(:user, user)

    refute conn.halted
  end

  test "no user/resource provided" do
    conn = build_conn(:get, "/any", %{})
    refute conn.halted
  end

  test "no user provided", %{endpoint: endpoint} do
    conn =
      build_conn(:get, "/any", %{})
      |> assign(:endpoint, endpoint)

    refute conn.halted
  end
end
