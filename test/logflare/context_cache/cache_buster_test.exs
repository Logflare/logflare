defmodule Logflare.ContextCache.CacheBusterTest do
  use Logflare.DataCase

  alias Cainophile.Changes.DeletedRecord
  alias Cainophile.Changes.Transaction
  alias Cainophile.Changes.UpdatedRecord
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.ConnectionManager
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryConnectionSup
  alias Logflare.ContextCache
  alias Logflare.ContextCache.CacheBuster
  alias Logflare.Sources

  setup do
    insert(:plan, name: "Free")
    user = insert(:user)
    source = insert(:source, user: user)
    [source: source, user: user]
  end

  test "cache buster", %{source: %{id: source_id, token: source_token}} do
    start_busters()

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

  for {scenario, record_type} <- [
        {"backend update", UpdatedRecord},
        {"backend delete", DeletedRecord}
      ] do
    test "#{scenario} from the WAL", %{user: user} do
      start_busters()
      test_pid = self()

      stub(Backends, :apply_backend_change_local, fn backend_id ->
        send(test_pid, {:applied, backend_id})
        :ok
      end)

      %{id: backend_id} = insert(:backend, user: user)
      record = %{"id" => Integer.to_string(backend_id), "user_id" => Integer.to_string(user.id)}

      change =
        struct(unquote(record_type),
          relation: {"public", "backends"},
          record: record,
          old_record: record
        )

      send(CacheBuster, %Transaction{changes: [change]})

      assert_receive {:applied, ^backend_id}, 1_000
    end
  end

  test "running read pool after a backend config update from the WAL", %{user: user} do
    start_busters()

    backend =
      insert(:backend,
        type: :clickhouse,
        user: user,
        config: %{
          url: "http://localhost:8123",
          port: 8123,
          database: "logflare_test",
          username: "logflare",
          password: "logflare",
          read_pool_size: 2
        }
      )

    on_exit(fn -> QueryConnectionSup.terminate_backend_local(backend.id) end)

    {:ok, _pid} =
      QueryConnectionSup.start_connection_manager(ConnectionManager.child_spec(backend))

    :ok = ConnectionManager.ensure_pool_started(backend)
    {:ok, _updated} = Backends.update_backend(backend, %{config: %{read_pool_size: 3}})

    change = %UpdatedRecord{
      relation: {"public", "backends"},
      record: %{"id" => Integer.to_string(backend.id), "user_id" => Integer.to_string(user.id)}
    }

    send(CacheBuster, %Transaction{changes: [change]})
    backend_id = backend.id

    TestUtils.retry_assert(fn ->
      :ok = ConnectionManager.ensure_pool_started(backend)

      assert [{^backend_id, nil, _pool_pid, 3}] =
               for(
                 pool <- QueryConnectionSup.list_read_pools(),
                 elem(pool, 0) == backend_id,
                 do: pool
               )
    end)
  end

  test "a default backend update refreshes the owner's default ingest lookup from the WAL user_id",
       %{
         user: user
       } do
    start_busters()

    stub(Backends, :apply_backend_change_local, fn _backend_id -> :ok end)

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
    start_busters()

    stub(Backends, :apply_backend_change_local, fn _backend_id -> :ok end)

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

  defp start_busters do
    for child_spec <- ContextCache.Supervisor.buster_specs() do
      start_supervised!(child_spec)
    end
  end
end
