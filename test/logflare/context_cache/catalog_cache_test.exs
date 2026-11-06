defmodule Logflare.ContextCache.CatalogCacheTest do
  use ExUnit.Case, async: false

  alias Logflare.Cache.CachexOps
  alias Logflare.ContextCache.CatalogCache
  alias Logflare.Repo

  @cache __MODULE__.Entries

  setup do
    start_supervised!({Cachex, name: @cache})
    Cachex.clear!(CatalogCache)
    on_exit(fn -> Cachex.clear!(CatalogCache) end)
    :ok
  end

  test "generation metadata exposes the same statistics as other context caches" do
    assert {:ok, stats} = Cachex.stats(CatalogCache)
    assert is_map(stats)
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

  test "retains empty catalogs until owner invalidation" do
    assert [] = CatalogCache.fetch(@cache, :owner, fn -> [] end)
    assert Cachex.size!(@cache) == 1

    assert [] = CatalogCache.fetch(@cache, :owner, fn -> flunk("cached empty catalog missed") end)
    keys = CatalogCache.keys_to_bust(@cache, [:owner])
    assert Cachex.size!(@cache) == 1
    assert {:ok, 1} = CachexOps.delete_keys(@cache, keys)
    assert [%{id: 1}] = CatalogCache.fetch(@cache, :owner, fn -> [%{id: 1}] end)
  end

  for snapshot <- [[], [%{id: 1, name: "old"}]] do
    @snapshot snapshot

    test "invalidation fences pending #{inspect(snapshot)} fills and rereads the primary" do
      test_pid = self()
      counter = start_supervised!({Agent, fn -> 0 end})

      loader = fn ->
        case Agent.get_and_update(counter, &{&1, &1 + 1}) do
          0 ->
            send(test_pid, {:snapshot_read, self()})

            receive do
              :release -> @snapshot
            end

          _next_read ->
            [%{id: 1, name: "new"}]
        end
      end

      task = Task.async(fn -> CatalogCache.fetch(@cache, :owner, loader) end)
      assert_receive {:snapshot_read, worker}, 1_000
      assert {:ok, 0} = CachexOps.delete_keys(@cache, CatalogCache.keys_to_bust(@cache, [:owner]))
      send(worker, :release)

      assert [%{name: "new"}] = Task.await(task)
      assert [%{name: "new"}] = CatalogCache.fetch(@cache, :owner, loader)
    end
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

    assert {:ok, 1} = CachexOps.delete_keys(@cache, CatalogCache.keys_to_bust(@cache, [1, 1]))

    assert [%{name: "new"}] =
             CatalogCache.fetch(@cache, 1, fn -> [%{id: 1, name: "new"}] end)

    assert [%{name: "old"}] =
             CatalogCache.fetch(@cache, 2, fn -> [%{id: 2, name: "new"}] end)
  end
end
