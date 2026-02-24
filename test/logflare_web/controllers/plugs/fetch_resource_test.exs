defmodule LogflareWeb.Plugs.FetchResourceTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  alias LogflareWeb.Plugs.FetchResource
  alias Logflare.Sources.Source
  alias Logflare.Endpoints.Query

  setup do
    insert(:plan)
    user = insert(:user)
    {:ok, user: user}
  end

  describe "source" do
    setup %{user: user} do
      source = insert(:source, user: user)
      {:ok, source: source}
    end

    test "fetches a source by name", %{source: %Source{id: id, name: name}, user: user} do
      conn =
        build_conn(:post, "/logs", %{"source_name" => name})
        |> assign(:user, user)

      assert_plug_call_works_for_source(conn, id)
    end

    test "fetches a source by collection name", %{source: %Source{id: id, name: name}, user: user} do
      conn =
        build_conn(:post, "/logs", %{"collection_name" => name})
        |> assign(:user, user)

      assert_plug_call_works_for_source(conn, id)
    end

    test "fetches a source from source param", %{source: %Source{id: id, token: token}} do
      conn = build_conn(:post, "/logs", %{"source" => Atom.to_string(token)})

      assert_plug_call_works_for_source(conn, id)
    end

    test "fetches a source from collection param", %{source: %Source{id: id, token: token}} do
      conn = build_conn(:post, "/logs", %{"collection" => Atom.to_string(token)})
      assert_plug_call_works_for_source(conn, id)
    end

    test "fetches a source from x-source header", %{source: %Source{id: id, token: token}} do
      conn =
        build_conn(:post, "/logs", %{})
        |> put_req_header("x-source", Atom.to_string(token))

      assert_plug_call_works_for_source(conn, id)
    end

    test "fetches a source from x-collection header", %{source: %Source{id: id, token: token}} do
      conn =
        build_conn(:post, "/logs", %{})
        |> put_req_header("x-collection", Atom.to_string(token))

      assert_plug_call_works_for_source(conn, id)
    end

    test "returns nil for invalid source token" do
      conn = build_conn(:post, "/logs", %{"source" => "not-a-uuid"})
      refute Map.get(conn.assigns, :source)

      conn =
        conn
        |> assign(:resource_type, :source)
        |> FetchResource.call(%{})

      refute Map.get(conn.assigns, :source)
      refute conn.halted
    end

    defp assert_plug_call_works_for_source(conn, source_id) do
      refute Map.get(conn.assigns, :source)

      conn =
        conn
        |> assign(:resource_type, :source)
        |> FetchResource.call(%{})

      assert %Source{id: ^source_id} = Map.get(conn.assigns, :source)
      refute conn.halted
    end
  end

  describe "endpoints" do
    setup %{user: user} do
      endpoint = insert(:endpoint, user: user)
      {:ok, endpoint: endpoint}
    end

    test "fetches an endpoint by name or token", %{endpoint: %Query{id: id, token: token}} do
      conn =
        build_conn(:get, "/endpoints/query/#{token}", %{
          "token_or_name" => token
        })

      assert_plug_call_works_for_endpoint(conn, id)
    end

    test "fetches an endpoint by token when user is present", %{endpoint: endpoint, user: user} do
      conn =
        build_conn(:get, "/endpoints/query/#{endpoint.token}", %{
          "token_or_name" => endpoint.token
        })
        |> assign(:user, user)

      assert_plug_call_works_for_endpoint(conn, endpoint.id)
    end

    test "fetches an endpoint by name when user is present", %{user: user} do
      endpoint = insert(:endpoint, user: user, name: "Endpoint Name")

      conn =
        build_conn(:get, "/endpoints/query/#{endpoint.name}", %{"token_or_name" => endpoint.name})
        |> assign(:user, user)

      assert_plug_call_works_for_endpoint(conn, endpoint.id)
    end

    test "fetches an endpoint by name param", %{endpoint: endpoint, user: user} do
      conn =
        build_conn(:get, "/endpoints/query/#{endpoint.name}", %{"name" => endpoint.name})
        |> assign(:user, user)

      assert_plug_call_works_for_endpoint(conn, endpoint.id)
    end

    test "returns nil for unknown endpoint name" do
      conn = build_conn(:get, "/endpoints/query/unknown", %{"token_or_name" => "unknown"})

      conn =
        conn
        |> assign(:resource_type, :endpoint)
        |> FetchResource.call(%{})

      refute Map.get(conn.assigns, :endpoint)
      refute conn.halted
    end

    defp assert_plug_call_works_for_endpoint(conn, endpoint_id) do
      refute Map.get(conn.assigns, :endpoint)

      conn =
        conn
        |> assign(:resource_type, :endpoint)
        |> FetchResource.call(%{})

      assert %Query{id: ^endpoint_id} = Map.get(conn.assigns, :endpoint)
      refute conn.halted
    end
  end

  test "returns conn unchanged when resource_type is missing" do
    conn = build_conn(:get, "/logs", %{})

    assert FetchResource.call(conn, %{}) == conn
  end
end
