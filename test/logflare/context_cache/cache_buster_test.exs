defmodule Logflare.ContextCache.CacheBusterTest do
  use Logflare.DataCase

  alias Cainophile.Changes.DeletedRecord
  alias Cainophile.Changes.NewRecord
  alias Cainophile.Changes.Transaction
  alias Cainophile.Changes.UpdatedRecord
  alias Logflare.Backends
  alias Logflare.ContextCache
  alias Logflare.ContextCache.CacheBuster
  alias Logflare.Endpoints
  alias Logflare.Sources
  alias Logflare.Sources.Catalog

  setup do
    insert(:plan, name: "Free")
    user = insert(:user)
    source = insert(:source, user: user)

    for child_spec <- ContextCache.Supervisor.buster_specs() do
      start_supervised!(child_spec)
    end

    Cachex.clear!(Catalog.Cache)
    Cachex.clear!(Endpoints.Cache)

    on_exit(fn ->
      Cachex.clear!(Catalog.Cache)
      Cachex.clear!(Endpoints.Cache)
    end)

    [source: source, user: user]
  end

  test "cache buster", %{source: %{id: source_id, token: source_token}} do
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

  test "endpoint inserts bust negative lookups and owner catalogs", %{user: %{id: user_id} = user} do
    endpoint = insert(:endpoint, user: user)
    Endpoints.Cache.list_by_user_id(user_id)

    change = %NewRecord{
      relation: {"public", "endpoint_queries"},
      record: %{"id" => Integer.to_string(endpoint.id), "user_id" => Integer.to_string(user_id)}
    }

    test_pid = self()

    Mimic.expect(ContextCache, :bust_keys, fn arg ->
      Mimic.call_original(ContextCache, :bust_keys, [arg])
      send(test_pid, arg)
    end)

    send(CacheBuster, %Transaction{changes: [change]})

    assert_receive [
                     {Endpoints, :not_found},
                     {Endpoints, [user_id: ^user_id]}
                   ],
                   500

    assert Cachex.size!(Endpoints.Cache) == 0
  end

  test "source updates bust the entity and owner catalog", %{
    source: %{id: source_id, token: source_token},
    user: %{id: user_id} = user
  } do
    Sources.Cache.get_by(token: source_token)
    Catalog.Cache.list_by_user(user)

    change = %UpdatedRecord{
      relation: {"public", "sources"},
      record: %{"id" => Integer.to_string(source_id), "user_id" => Integer.to_string(user_id)},
      old_record: %{}
    }

    assert_source_catalog_bust(change, source_id, user_id)
    assert Cachex.size!(Sources.Cache) == 0
    assert Cachex.size!(Catalog.Cache) == 0
  end

  test "source inserts bust negative entity lookups and owner catalogs", %{
    source: %{id: source_id},
    user: %{id: user_id}
  } do
    change = %NewRecord{
      relation: {"public", "sources"},
      record: %{"id" => Integer.to_string(source_id), "user_id" => Integer.to_string(user_id)}
    }

    assert_source_catalog_bust(change, :not_found, user_id)
  end

  test "source deletes bust the entity and owner catalog", %{
    source: %{id: source_id},
    user: %{id: user_id}
  } do
    change = %DeletedRecord{
      relation: {"public", "sources"},
      old_record: %{"id" => Integer.to_string(source_id), "user_id" => Integer.to_string(user_id)}
    }

    assert_source_catalog_bust(change, source_id, user_id)
  end

  defp assert_source_catalog_bust(change, source_key, user_id) do
    test_pid = self()

    Mimic.expect(ContextCache, :bust_keys, fn arg ->
      Mimic.call_original(ContextCache, :bust_keys, [arg])
      send(test_pid, arg)
    end)

    send(CacheBuster, %Transaction{changes: [change]})

    assert_receive [
                     {Sources, ^source_key},
                     {Catalog, [user_id: ^user_id]}
                   ],
                   500
  end
end
