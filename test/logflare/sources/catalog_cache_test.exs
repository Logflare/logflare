defmodule Logflare.Sources.Catalog.CacheTest do
  use Logflare.DataCase

  alias Logflare.ContextCache
  alias Logflare.Repo
  alias Logflare.Sources.Catalog
  alias Logflare.Sources.Source

  setup do
    Cachex.clear!(Catalog.Cache)
    user = insert(:user)

    on_exit(fn -> Cachex.clear!(Catalog.Cache) end)

    %{user: user}
  end

  test "caches lightweight source catalogs by user", %{user: user} do
    source = insert(:source, user: user, name: "original")

    assert [%Source{id: source_id, name: "original", token: token}] =
             Catalog.Cache.list_by_user(user)

    assert source_id == source.id
    assert token == source.token

    source
    |> Ecto.Changeset.change(name: "updated")
    |> Repo.update!()

    assert [%Source{name: "original"}] = Catalog.Cache.list_by_user(user.id)

    assert {:ok, 1} = ContextCache.bust_keys([{Catalog, user_id: user.id}])
    assert [%Source{name: "updated"}] = Catalog.Cache.list_by_user(user.id)
  end

  test "bypasses the catalog cache inside transactions", %{user: user} do
    source = insert(:source, user: user, name: "original")
    assert [%Source{name: "original"}] = Catalog.Cache.list_by_user(user.id)

    Repo.transaction(fn ->
      source
      |> Ecto.Changeset.change(name: "updated")
      |> Repo.update!()

      assert [%Source{name: "updated"}] = Catalog.Cache.list_by_user(user.id)
    end)

    assert [%Source{name: "original"}] = Catalog.Cache.list_by_user(user.id)
  end

  test "does not cache empty catalogs", %{user: user} do
    assert [] = Catalog.Cache.list_by_user(user.id)
    assert Cachex.size!(Catalog.Cache) == 0

    source = insert(:source, user: user)

    assert [%Source{id: source_id}] = Catalog.Cache.list_by_user(user.id)
    assert source_id == source.id
  end

  test "invalidating a catalog removes deleted sources", %{user: user} do
    source = insert(:source, user: user)
    assert [%Source{id: source_id}] = Catalog.Cache.list_by_user(user.id)
    assert source_id == source.id

    Repo.delete!(source)

    assert [%Source{id: ^source_id}] = Catalog.Cache.list_by_user(user.id)
    assert {:ok, 1} = ContextCache.bust_keys([{Catalog, user_id: user.id}])
    assert [] = Catalog.Cache.list_by_user(user.id)
  end
end
