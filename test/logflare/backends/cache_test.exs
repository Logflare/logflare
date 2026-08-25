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
