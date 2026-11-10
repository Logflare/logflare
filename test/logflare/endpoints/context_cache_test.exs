defmodule Logflare.Endpoints.ContextCacheTest do
  use Logflare.DataCase

  alias Logflare.ContextCache
  alias Logflare.Endpoints
  alias Logflare.Endpoints.EndpointQuery
  alias Logflare.Repo

  setup do
    Cachex.clear!(Endpoints.Cache)
    user = insert(:user)

    on_exit(fn -> Cachex.clear!(Endpoints.Cache) end)

    %{user: user}
  end

  test "catalog availability errors propagate through execution and previews", %{user: user} do
    insert(:endpoint, user: user, name: "referenced", query: "SELECT 1")
    endpoint = insert(:endpoint, user: user, query: "SELECT * FROM referenced")

    stub(Endpoints, :list_endpoints_by, fn _ ->
      raise DBConnection.ConnectionError, message: "primary unavailable"
    end)

    assert {:error, :database_unavailable} = Endpoints.Cache.list_by_user_id(user.id)
    assert {:error, :database_unavailable} = Endpoints.run_query(endpoint)
    assert {:error, :database_unavailable} = Endpoints.get_transformed_query(endpoint)
  end

  test "caches endpoint catalogs by user and busts them by endpoint id", %{user: user} do
    endpoint = insert(:endpoint, user: user, name: "original")

    assert [%EndpointQuery{id: endpoint_id, name: "original"}] =
             Endpoints.Cache.list_by_user_id(user.id)

    endpoint
    |> Ecto.Changeset.change(name: "updated")
    |> Repo.update!()

    assert [%EndpointQuery{name: "original"}] = Endpoints.Cache.list_by_user_id(user.id)

    assert {:ok, 1} = ContextCache.bust_keys([{Endpoints, endpoint_id}])
    assert [%EndpointQuery{name: "updated"}] = Endpoints.Cache.list_by_user_id(user.id)
  end

  test "caches empty endpoint catalogs until owner invalidation", %{user: user} do
    assert [] = Endpoints.Cache.list_by_user_id(user.id)
    assert Cachex.size!(Endpoints.Cache) == 1

    endpoint = insert(:endpoint, user: user)

    assert [] = Endpoints.Cache.list_by_user_id(user.id)
    assert {:ok, 1} = ContextCache.bust_keys([{Endpoints, user_id: user.id}])
    assert [%EndpointQuery{id: endpoint_id}] = Endpoints.Cache.list_by_user_id(user.id)
    assert endpoint_id == endpoint.id
  end

  test "bypasses the endpoint catalog cache inside transactions", %{user: user} do
    endpoint = insert(:endpoint, user: user, name: "original")
    assert [%EndpointQuery{name: "original"}] = Endpoints.Cache.list_by_user_id(user.id)

    Repo.transaction(fn ->
      endpoint
      |> Ecto.Changeset.change(name: "updated")
      |> Repo.update!()

      assert [%EndpointQuery{name: "updated"}] = Endpoints.Cache.list_by_user_id(user.id)
    end)

    assert [%EndpointQuery{name: "original"}] = Endpoints.Cache.list_by_user_id(user.id)
  end
end
