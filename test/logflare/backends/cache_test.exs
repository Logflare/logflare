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

      Backends, {:list_backends, 1}, [[source_id: source_id]]
      when source_id == source.id ->
        Mimic.call_original(Logflare.ContextCache, :apply_fun_primary, [
          Backends,
          {:list_backends, 1},
          [[source_id: source_id]]
        ])
    end)

    assert %{id: backend_id} = Backends.Cache.get_backend(backend.id)
    assert backend_id == backend.id

    assert [%{id: ^backend_id}] =
             Backends.Cache.list_enabled_backends(source_id: source.id)
  end

  test "list_enabled_backends/1 filters the existing unfiltered cache entry", %{
    backend: backend,
    source: source
  } do
    disabled_backend = insert(:backend, sources: [source], enabled: false)

    assert [%{id: backend_id}] = Backends.Cache.list_enabled_backends(source_id: source.id)
    assert backend_id == backend.id

    cache_key = {:list_backends, [[source_id: source.id]]}
    assert {:cached, cached_backends} = Cachex.get!(Backends.Cache, cache_key)

    assert Enum.sort(Enum.map(cached_backends, & &1.id)) ==
             Enum.sort([backend.id, disabled_backend.id])

    refute Cachex.get!(
             Backends.Cache,
             {:list_enabled_backends, [[source_id: source.id]]}
           )
  end

  test "list_enabled_backends/1 treats a legacy cached backend as enabled", %{
    backend: backend,
    source: source
  } do
    legacy_backend = Map.delete(backend, :enabled)
    cache_key = {:list_backends, [[source_id: source.id]]}
    Cachex.put!(Backends.Cache, cache_key, {:cached, [legacy_backend]})

    assert [cached_backend] = Backends.Cache.list_enabled_backends(source_id: source.id)
    assert cached_backend.id == backend.id
    refute Map.has_key?(cached_backend, :enabled)
  end
end
