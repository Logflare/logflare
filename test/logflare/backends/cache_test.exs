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

  test "limits unique ingesting backends by latest source activity", %{user: user} do
    now = NaiveDateTime.utc_now()

    newest_source = insert(:source, user: user, log_events_updated_at: now)

    oldest_source =
      insert(:source,
        user: user,
        log_events_updated_at: NaiveDateTime.shift(now, hour: -2)
      )

    older_source =
      insert(:source,
        user: user,
        log_events_updated_at: NaiveDateTime.shift(now, hour: -1)
      )

    older_backend = insert(:backend, user: user, sources: [older_source])
    newest_backend = insert(:backend, user: user, sources: [newest_source, oldest_source])
    newest_backend_id = newest_backend.id
    older_backend_id = older_backend.id

    assert [%{id: ^newest_backend_id}] = Backends.list_backends(ingesting: true, limit: 1)

    assert [%{id: ^newest_backend_id}, %{id: ^older_backend_id}] =
             Backends.list_backends(ingesting: true)

    assert [%{id: ^newest_backend_id}] =
             Backends.list_backends(
               ingesting: true,
               has_sources_or_rules: true,
               limit: 1
             )

    assert [%{id: ^newest_backend_id}] =
             Backends.list_backends(
               has_sources_or_rules: true,
               ingesting: true,
               limit: 1
             )

    assert [%{id: ^newest_backend_id}] =
             Backends.list_backends_by_user_access(user, ingesting: true, limit: 1)
  end

  test "breaks ingesting backend activity ties by id", %{user: user} do
    active_at = NaiveDateTime.utc_now()
    first_source = insert(:source, user: user, log_events_updated_at: active_at)
    second_source = insert(:source, user: user, log_events_updated_at: active_at)
    first_backend = insert(:backend, sources: [first_source])
    _second_backend = insert(:backend, sources: [second_source])
    first_backend_id = first_backend.id

    assert [%{id: ^first_backend_id}] = Backends.list_backends(ingesting: true, limit: 1)
  end

  test "warmer", %{user: user} do
    assert :ignore = CacheWarmer.execute(nil)

    source =
      insert(:source,
        user: user,
        log_events_updated_at: NaiveDateTime.shift(NaiveDateTime.utc_now(), hour: -2)
      )

    backend = insert(:backend, sources: [source])

    assert :ignore = CacheWarmer.execute(nil)

    Backends
    |> reject(:get_backend, 1)

    assert Backends.Cache.get_backend(backend.id)
  end
end
