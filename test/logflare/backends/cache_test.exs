defmodule Logflare.Backends.CacheTest do
  @moduledoc false
  import ExUnit.CaptureLog

  alias Logflare.Backends
  alias Logflare.Backends.CacheWarmer
  alias Logflare.Cluster.Utils, as: ClusterUtils
  alias Logflare.ContextCache.CacheBusterWorker
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

  test "list_default_ingest_backends/1 caches per account and is busted on flag change", %{
    user: user
  } do
    backend =
      insert(:backend,
        user: user,
        type: :clickhouse,
        config: %{url: "http://localhost", database: "test", port: 8123}
      )

    stub(ClusterUtils, :rpc_multicast, fn mod, fun, args ->
      apply(mod, fun, args)
      :ok
    end)

    assert Backends.Cache.list_default_ingest_backends(user.id) == []

    assert {:ok, _} = Backends.update_backend(backend, %{default_ingest?: true})
    assert [%{id: id}] = Backends.Cache.list_default_ingest_backends(user.id)
    assert id == backend.id
  end

  test "list_default_ingest_backends/1 fills a miss from the primary", %{user: user} do
    default =
      insert(:backend,
        user: user,
        type: :clickhouse,
        default_ingest?: true,
        config: %{url: "http://localhost", database: "test", port: 8123}
      )

    ref = :telemetry_test.attach_event_handlers(self(), [[:logflare, :repo, :replica_route]])
    on_exit(fn -> :telemetry.detach(ref) end)

    assert [_default] = Backends.Cache.list_default_ingest_backends(user.id)
    refute_received {[:logflare, :repo, :replica_route], ^ref, _measurements, _metadata}

    assert Backends.Cache.get_backend(default.id)
    assert_received {[:logflare, :repo, :replica_route], ^ref, %{count: 1}, _metadata}
  end

  test "linking a default backend to another source keeps the account default lookup", %{
    user: user
  } do
    default =
      insert(:backend,
        user: user,
        type: :clickhouse,
        default_ingest?: true,
        config: %{url: "http://localhost", database: "test", port: 8123}
      )

    other_source = insert(:source, user: user)

    assert [%{id: id}] = Backends.Cache.list_default_ingest_backends(user.id)
    assert id == default.id

    assert {:ok, _} = Backends.update_source_backends(other_source, [default])

    assert [%{id: ^id}] = Backends.Cache.list_default_ingest_backends(user.id)
  end

  test "a backend WAL bust re-puts the cached account default lookup from the primary", %{
    user: user
  } do
    stub(Backends, :sync_backend_across_cluster, fn _backend_id -> :ok end)

    backend =
      insert(:backend,
        user: user,
        type: :clickhouse,
        default_ingest?: true,
        config: %{url: "http://localhost", database: "test", port: 8123}
      )

    key = {:list_default_ingest_backends, [user.id]}
    stale = [%{backend | name: "stale"}]
    assert ^stale = Backends.Cache.fetch(key, fn -> stale end)

    assert {:noreply, _} =
             CacheBusterWorker.handle_cast({:to_bust, [{Backends, backend.id}], []}, %{})

    TestUtils.retry_assert(fn ->
      assert key in Enum.to_list(Backends.Cache.keys_to_bust(id: backend.id))
    end)

    assert [%{id: id, name: name, default_ingest?: true}] =
             Backends.Cache.fetch(key, fn -> flunk("default lookup not cached") end)

    assert id == backend.id
    assert name == backend.name
  end

  test "a backend WAL bust leaves accounts without a cached default lookup untouched", %{
    user: user
  } do
    test_pid = self()
    stub(Backends, :sync_backend_across_cluster, fn _backend_id -> :ok end)

    stub(Backends, :refresh_default_ingest_backends_cache, fn user_id ->
      send(test_pid, {:refreshed, user_id})
      :ok
    end)

    backend =
      insert(:backend,
        user: user,
        type: :clickhouse,
        default_ingest?: true,
        config: %{url: "http://localhost", database: "test", port: 8123}
      )

    assert Backends.cached_default_ingest_user_ids([backend.id]) == []

    assert {:noreply, _} =
             CacheBusterWorker.handle_cast({:to_bust, [{Backends, backend.id}], []}, %{})

    assert Backends.cached_default_ingest_user_ids([backend.id]) == []
    refute_received {:refreshed, _user_id}
  end

  test "a backend WAL bust refreshes the owner lookup cached before the backend became a default",
       %{user: user} do
    stub(Backends, :sync_backend_across_cluster, fn _backend_id -> :ok end)

    backend =
      insert(:backend,
        user: user,
        type: :clickhouse,
        config: %{url: "http://localhost", database: "test", port: 8123}
      )

    assert [] = Backends.Cache.list_default_ingest_backends(user.id)

    backend |> Ecto.Changeset.change(default_ingest?: true) |> Repo.update!()

    assert {:noreply, _} =
             CacheBusterWorker.handle_cast({:to_bust, [{Backends, backend.id}], [user.id]}, %{})

    TestUtils.retry_assert(fn ->
      assert [%{id: id}] = Backends.Cache.list_default_ingest_backends(user.id)
      assert id == backend.id
    end)
  end

  test "a backend WAL bust keeps refreshing the remaining owners when one refresh exits", %{
    user: user,
    backend: backend
  } do
    test_pid = self()
    other_user = insert(:user)
    failing_id = user.id
    other_id = other_user.id
    stub(Backends, :sync_backend_across_cluster, fn _backend_id -> :ok end)

    stub(Backends, :refresh_default_ingest_backends_cache, fn
      ^failing_id -> exit(:timeout)
      user_id -> send(test_pid, {:refreshed, user_id})
    end)

    log =
      capture_log([metadata: [:user_id]], fn ->
        assert {:noreply, _} =
                 CacheBusterWorker.handle_cast(
                   {:to_bust, [{Backends, backend.id}], [failing_id, other_id]},
                   %{}
                 )

        assert_receive {:refreshed, ^other_id}
      end)

    assert log =~ "Default ingest lookup refresh exited"
    assert log =~ "user_id=#{failing_id}"
  end
end
