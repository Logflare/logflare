defmodule Logflare.Backends.CacheTest do
  @moduledoc false
  alias Logflare.Backends
  alias Logflare.Backends.CacheWarmer
  use Logflare.DataCase

  setup do
    insert(:plan)
    user = insert(:user)
    source = insert(:source, user: user)

    backend =
      insert(:backend,
        sources: [source]
      )

    {:ok, backend: backend, source: source, user: user}
  end

  test "clear_list_backends_cache/1 refreshes cached source backends", %{
    backend: backend,
    source: source
  } do
    backend_id = backend.id

    # Prime the cache with the backend currently associated with the source.
    assert [%{id: ^backend_id}] = Backends.Cache.list_backends(source_id: source.id)

    # Remove the association directly so the cached result remains stale.
    assert {1, nil} =
             Repo.delete_all(
               from(sb in "sources_backends",
                 where: sb.source_id == ^source.id and sb.backend_id == ^backend.id
               )
             )

    assert [%{id: ^backend_id}] = Backends.Cache.list_backends(source_id: source.id)

    # Clearing the source-specific entry makes the next lookup reflect the database.
    assert :ok = Backends.clear_list_backends_cache(source.id)
    assert [] = Backends.Cache.list_backends(source_id: source.id)
  end

  test "warmer", %{user: user} do
    assert {:ok, []} = CacheWarmer.execute(nil)

    source =
      insert(:source,
        user: user,
        log_events_updated_at: NaiveDateTime.shift(NaiveDateTime.utc_now(), hour: -2)
      )

    backend = insert(:backend, sources: [source])

    assert {:ok, [_ | _] = pairs} = CacheWarmer.execute(nil)
    assert {:ok, true} = Cachex.put_many(Backends.Cache, pairs)

    Backends
    |> reject(:get_backend, 1)

    assert Backends.Cache.get_backend(backend.id)
  end

  test "warmer discards a snapshot loaded across an invalidation", %{backend: backend} do
    cache_key = {:get_backend, [backend.id]}

    expect(Backends, :list_backends, fn ingesting: true, limit: 1_000 ->
      Logflare.ContextCache.Gossip.record_cache_tombstones(Backends.Cache, [cache_key])
      [backend]
    end)

    assert {:ok, []} = CacheWarmer.execute(nil)
  end

  test "consistent reads reject a warmer snapshot inserted after invalidation", %{
    backend: backend
  } do
    expect(Backends, :list_backends, fn ingesting: true, limit: 1_000 -> [backend] end)
    assert {:ok, [{cache_key, _value}] = pairs} = CacheWarmer.execute(nil)

    backend
    |> Ecto.Changeset.change(enabled: false)
    |> Logflare.Repo.update!()

    Logflare.ContextCache.Gossip.record_cache_tombstones(Backends.Cache, [cache_key])
    Cachex.put_many(Backends.Cache, pairs)

    refute Backends.Cache.get_backend(backend.id).enabled
  end

  test "backend cache misses use the primary-backed context cache path", %{
    backend: backend,
    source: source
  } do
    reject(Logflare.ContextCache, :apply_fun, 3)

    expect(Logflare.ContextCache, :apply_fun_primary, 2, fn
      Backends, {:get_backend, 1}, [backend_id] when backend_id == backend.id ->
        Mimic.call_original(Logflare.ContextCache, :apply_fun_primary, [
          Backends,
          {:get_backend, 1},
          [backend_id]
        ])

      Backends, {:list_backends, 1}, [[source_id: source_id, enabled: true]]
      when source_id == source.id ->
        Mimic.call_original(Logflare.ContextCache, :apply_fun_primary, [
          Backends,
          {:list_backends, 1},
          [[source_id: source_id, enabled: true]]
        ])
    end)

    assert %{id: backend_id} = Backends.Cache.get_backend(backend.id)
    assert backend_id == backend.id

    assert [%{id: ^backend_id}] =
             Backends.Cache.list_backends(source_id: source.id, enabled: true)
  end

  test "reconciliation refreshes primed enabled-only source and rule caches", %{
    backend: backend,
    source: source
  } do
    insert(:rule, backend: backend, source: source)

    disabled_backend =
      backend
      |> Ecto.Changeset.change(enabled: false)
      |> Logflare.Repo.update!()

    Backends.clear_list_backends_cache(source.id)

    assert [] = Backends.Cache.list_backends(source_id: source.id, enabled: true)
    assert [] = Backends.Cache.list_backends(rules_source_id: source.id, enabled: true)

    disabled_backend
    |> Ecto.Changeset.change(enabled: true)
    |> Logflare.Repo.update!()

    assert [] = Backends.Cache.list_backends(source_id: source.id, enabled: true)
    assert [] = Backends.Cache.list_backends(rules_source_id: source.id, enabled: true)

    assert :ok = Backends.reconcile_backend_local(backend.id)

    assert [%{id: backend_id}] =
             Backends.Cache.list_backends(source_id: source.id, enabled: true)

    assert backend_id == backend.id

    assert [%{id: ^backend_id}] =
             Backends.Cache.list_backends(rules_source_id: source.id, enabled: true)
  end
end
