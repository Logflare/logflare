defmodule Logflare.ContextCache.CacheBusterTest do
  use Logflare.DataCase

  import ExUnit.CaptureLog

  alias Cainophile.Changes.DeletedRecord
  alias Cainophile.Changes.Transaction
  alias Cainophile.Changes.UpdatedRecord
  alias Logflare.Backends
  alias Logflare.ContextCache
  alias Logflare.ContextCache.CacheBuster
  alias Logflare.ContextCache.Tombstones
  alias Logflare.Sources

  setup do
    insert(:plan, name: "Free")
    user = insert(:user)
    source = insert(:source, user: user)
    [source: source, user: user]
  end

  test "cache buster", %{source: %{id: source_id, token: source_token}} do
    for child_spec <- ContextCache.Supervisor.buster_specs() do
      start_supervised!(child_spec)
    end

    Sources.Cache.get_by(token: source_token)
    assert Cachex.size!(Sources.Cache) == 1

    change = %DeletedRecord{
      relation: {"public", "sources"},
      old_record: %{"id" => Integer.to_string(source_id)}
    }

    test_pid = self()

    Mimic.expect(ContextCache, :bust_keys, fn arg ->
      Mimic.call_original(ContextCache, :bust_keys, [arg])
      send(test_pid, arg)
    end)

    send(CacheBuster, %Transaction{changes: [change]})
    assert_receive [{Sources, ^source_id}], 500
    assert Cachex.size!(Sources.Cache) == 0
  end

  test "a default backend update refreshes the owner's default ingest lookup from the WAL user_id",
       %{
         user: user
       } do
    for child_spec <- ContextCache.Supervisor.buster_specs() do
      start_supervised!(child_spec)
    end

    stub(Backends, :sync_backend_across_cluster, fn _backend_id -> :ok end)

    backend =
      insert(:backend,
        user: user,
        type: :clickhouse,
        config: %{url: "http://localhost", database: "test", port: 8123}
      )

    assert [] = Backends.Cache.list_default_ingest_backends(user.id)
    backend |> Ecto.Changeset.change(default_ingest?: true) |> Repo.update!()

    change = %UpdatedRecord{
      relation: {"public", "backends"},
      record: %{
        "id" => Integer.to_string(backend.id),
        "user_id" => Integer.to_string(user.id),
        "default_ingest" => "t"
      }
    }

    send(CacheBuster, %Transaction{changes: [change]})

    TestUtils.retry_assert(fn ->
      assert [%{id: id}] = Backends.Cache.list_default_ingest_backends(user.id)
      assert id == backend.id
    end)
  end

  test "a non-default backend update does not refresh the owner's default ingest lookup", %{
    user: user
  } do
    for child_spec <- ContextCache.Supervisor.buster_specs() do
      start_supervised!(child_spec)
    end

    stub(Backends, :sync_backend_across_cluster, fn _backend_id -> :ok end)

    backend =
      insert(:backend,
        user: user,
        type: :webhook,
        config: %{url: "http://test.com"}
      )

    assert [] = Backends.Cache.list_default_ingest_backends(user.id)
    test_pid = self()

    stub(Backends, :refresh_default_ingest_backends_cache, fn user_id ->
      send(test_pid, {:refreshed, user_id})
      :ok
    end)

    Mimic.expect(ContextCache, :bust_keys, fn arg ->
      result = Mimic.call_original(ContextCache, :bust_keys, [arg])
      send(test_pid, {:busted, arg})
      result
    end)

    change = %UpdatedRecord{
      relation: {"public", "backends"},
      record: %{
        "id" => Integer.to_string(backend.id),
        "user_id" => Integer.to_string(user.id),
        "default_ingest" => "f"
      }
    }

    send(CacheBuster, %Transaction{changes: [change]})

    backend_id = backend.id
    assert_receive {:busted, [{Backends, ^backend_id}]}, 500
    refute_receive {:refreshed, _user_id}, 300
  end
  test "backend WAL updates reconcile locally without a generic cache bust" do
    for child_spec <- ContextCache.Supervisor.buster_specs() do
      start_supervised!(child_spec)
    end

    backend_id = System.unique_integer([:positive])
    test_pid = self()
    Cachex.clear!(Tombstones.Cache)

    Mimic.expect(Backends, :reconcile_backend_local, fn id ->
      send(test_pid, {:backend_reconciled, id})
      :ok
    end)

    Mimic.reject(Backends, :sync_backend_across_cluster, 1)
    Mimic.reject(ContextCache, :bust_keys, 1)

    change = %UpdatedRecord{
      relation: {"public", "backends"},
      old_record: %{"id" => Integer.to_string(backend_id)},
      record: %{"id" => Integer.to_string(backend_id)}
    }

    send(CacheBuster, %Transaction{changes: [change]})
    assert_receive {:backend_reconciled, ^backend_id}, 500
    refute Tombstones.Cache.tombstoned?(Backends.Cache, backend_id)
  end

  test "retries a failed backend WAL reconciliation" do
    for child_spec <- ContextCache.Supervisor.buster_specs() do
      start_supervised!(child_spec)
    end

    backend_id = System.unique_integer([:positive])
    test_pid = self()

    Mimic.expect(Backends, :reconcile_backend_local, fn id ->
      send(test_pid, {:backend_reconciliation_failed, id})
      raise "transient reconciliation failure"
    end)

    Mimic.expect(Backends, :reconcile_backend_local, fn id ->
      send(test_pid, {:backend_reconciliation_retried, id})
      :ok
    end)

    change = %UpdatedRecord{
      relation: {"public", "backends"},
      old_record: %{"id" => Integer.to_string(backend_id)},
      record: %{"id" => Integer.to_string(backend_id)}
    }

    log =
      capture_log(fn ->
        send(CacheBuster, %Transaction{changes: [change]})

        assert_receive {:backend_reconciliation_failed, ^backend_id}, 500
        assert_receive {:backend_reconciliation_retried, ^backend_id}, 500
      end)

    assert log =~ "Backend reconciliation failed"
  end

  test "falls back to tombstoning and busting after reconciliation retries are exhausted" do
    for child_spec <- ContextCache.Supervisor.buster_specs() do
      start_supervised!(child_spec)
    end

    backend_id = System.unique_integer([:positive])
    test_pid = self()
    Cachex.clear!(Tombstones.Cache)

    Mimic.expect(Backends, :reconcile_backend_local, 4, fn id ->
      send(test_pid, {:backend_reconciliation_failed, id})
      raise "persistent reconciliation failure"
    end)

    Mimic.expect(ContextCache, :bust_keys, fn [{Backends, id}] = keys ->
      send(test_pid, {:backend_cache_busted, id})
      Mimic.call_original(ContextCache, :bust_keys, [keys])
    end)

    change = %UpdatedRecord{
      relation: {"public", "backends"},
      old_record: %{"id" => Integer.to_string(backend_id)},
      record: %{"id" => Integer.to_string(backend_id)}
    }

    capture_log(fn ->
      send(CacheBuster, %Transaction{changes: [change]})

      for _attempt <- 1..4 do
        assert_receive {:backend_reconciliation_failed, ^backend_id}, 1_000
      end

      assert_receive {:backend_cache_busted, ^backend_id}, 1_000
    end)

    assert Tombstones.Cache.tombstoned?(Backends.Cache, backend_id)
  end
end
