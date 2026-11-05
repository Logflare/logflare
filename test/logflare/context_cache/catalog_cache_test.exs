defmodule Logflare.ContextCache.CatalogCacheTest do
  use ExUnit.Case, async: false

  alias Logflare.ContextCache.CatalogCache
  alias Logflare.Repo

  @cache __MODULE__.Entries

  setup do
    start_supervised!({Cachex, name: @cache})
    Cachex.clear!(CatalogCache)
    on_exit(fn -> Cachex.clear!(CatalogCache) end)
    :ok
  end

  test "fills from the primary and reuses cached catalogs" do
    test_pid = self()

    loader = fn ->
      send(test_pid, {:loaded, Repo.get_dynamic_repo()})
      [%{id: 1}]
    end

    assert [%{id: 1}] = CatalogCache.fetch(@cache, :owner, loader)
    assert_receive {:loaded, Repo}
    assert [%{id: 1}] = CatalogCache.fetch(@cache, :owner, loader)
    refute_receive {:loaded, _repo}
  end

  test "does not retain empty catalogs" do
    assert [] = CatalogCache.fetch(@cache, :owner, fn -> [] end)
    assert Cachex.size!(@cache) == 0
    assert [%{id: 1}] = CatalogCache.fetch(@cache, :owner, fn -> [%{id: 1}] end)
  end

  test "invalidation fences pending fills and rereads the primary" do
    test_pid = self()
    counter = start_supervised!({Agent, fn -> 0 end})

    loader = fn ->
      case Agent.get_and_update(counter, &{&1, &1 + 1}) do
        0 ->
          send(test_pid, {:snapshot_read, self()})

          receive do
            :release -> [%{id: 1, name: "old"}]
          end

        _next_read ->
          [%{id: 1, name: "new"}]
      end
    end

    task = Task.async(fn -> CatalogCache.fetch(@cache, :owner, loader) end)
    assert_receive {:snapshot_read, worker}
    assert {:ok, 0} = CatalogCache.bust(@cache, [:owner])
    send(worker, :release)

    assert [%{name: "new"}] = Task.await(task)
    assert [%{name: "new"}] = CatalogCache.fetch(@cache, :owner, loader)
  end

  test "losing generation metadata never revives an older catalog" do
    assert [%{name: "old"}] =
             CatalogCache.fetch(@cache, :owner, fn -> [%{id: 1, name: "old"}] end)

    Cachex.clear!(CatalogCache)

    assert [%{name: "new"}] =
             CatalogCache.fetch(@cache, :owner, fn -> [%{id: 1, name: "new"}] end)
  end

  test "invalidation is scoped to the affected owner" do
    for owner <- [1, 2] do
      CatalogCache.fetch(@cache, owner, fn -> [%{id: owner, name: "old"}] end)
    end

    assert {:ok, 1} = CatalogCache.bust(@cache, [1, 1])

    assert [%{name: "new"}] =
             CatalogCache.fetch(@cache, 1, fn -> [%{id: 1, name: "new"}] end)

    assert [%{name: "old"}] =
             CatalogCache.fetch(@cache, 2, fn -> [%{id: 2, name: "new"}] end)
  end
end
