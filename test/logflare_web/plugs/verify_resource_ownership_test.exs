defmodule LogflareWeb.Plugs.VerifyResourceOwnershipTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  alias LogflareWeb.Plugs.VerifyResourceOwnership

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

    test "valid", %{conn: conn} do
      conn = VerifyResourceOwnership.call(conn, %{})

      refute conn.halted
    end

    test "invalid", %{conn: conn} do
      conn =
        conn
        |> assign(:user, insert(:user))
        |> VerifyResourceOwnership.call(%{})

      assert conn.halted
      assert conn.status == 401
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

    test "valid", %{conn: conn} do
      conn =
        conn
        |> VerifyResourceOwnership.call(%{})

      refute conn.halted
    end

    test "invalid", %{conn: conn} do
      conn =
        conn
        |> assign(:user, insert(:user))
        |> VerifyResourceOwnership.call(%{})

      assert conn.halted
      assert conn.status == 401
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
