defmodule LogflareWeb.Plugs.FetchResourceTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  alias LogflareWeb.Plugs.FetchResource
  alias Logflare.Sources.Source
  alias Logflare.Endpoints.Query

  setup do
    insert(:plan)
    user = insert(:user)
    endpoint = insert(:endpoint, user: user)
    source = insert(:source, user: user)
    {:ok, user: user, source: source, endpoint: endpoint}
  end

  describe "source" do
    setup %{source: source} do
      [conn: build_conn(:post, "/logs", %{"source" => Atom.to_string(source.token)})]
    end

    test "fetches a source", %{conn: conn, source: %Source{id: id}} do
      refute Map.get(conn.assigns, :source)

      conn =
        conn
        |> assign(:resource_type, :source)
        |> FetchResource.call(%{})

      assert %Source{id: ^id} = Map.get(conn.assigns, :source)
      refute conn.halted
    end
  end

  describe "endpoints" do
    setup %{endpoint: endpoint} do
      [
        conn:
          build_conn(:get, "/endpoints/query/#{endpoint.token}", %{
            "token_or_name" => endpoint.token
          })
      ]
    end

    test "fetches an endpoint by name or token", %{conn: conn, endpoint: %Query{id: id}} do
      refute Map.get(conn.assigns, :endpoint)

      conn =
        conn
        |> assign(:resource_type, :endpoint)
        |> FetchResource.call(%{})

      assert %Query{id: ^id} = Map.get(conn.assigns, :endpoint)
      refute conn.halted
    end
  end
end
