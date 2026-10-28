defmodule Logflare.ContextCache.CacheBusterTest do
  use Logflare.DataCase

  alias Cainophile.Changes.DeletedRecord
  alias Cainophile.Changes.Transaction
  alias Cainophile.Changes.UpdatedRecord
  alias Logflare.Backends
  alias Logflare.ContextCache
  alias Logflare.ContextCache.CacheBuster
  alias Logflare.Rules
  alias Logflare.Sources
  alias Logflare.Sources.SourceRouter.RulesTree

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

  @tag :capture_log
  test "backend toggles bust empty drain trees and retry failed relationship lookups", %{
    source: source,
    user: user
  } do
    for child_spec <- ContextCache.Supervisor.buster_specs() do
      start_supervised!(child_spec)
    end

    stub(Backends, :sync_backend_across_cluster, fn _backend_id -> :ok end)
    backend = insert(:backend, user: user, enabled: false)
    other = insert(:source, user: user)
    unrelated = insert(:source, user: user)
    first_rule = insert(:rule, source: source, backend: backend)
    insert(:rule, source: source, backend: backend)
    second_rule = insert(:rule, source: other, backend: backend)
    insert(:rule, source: unrelated)

    assert Rules.Cache.rules_tree_by_source_id(source.id) == []
    assert Rules.Cache.rules_tree_by_source_id(other.id) == []
    assert %{enabled: false} = Backends.Cache.get_backend(backend.id)
    unrelated_tree = Rules.Cache.rules_tree_by_source_id(unrelated.id)
    test_pid = self()

    stub(Rules, :list_source_ids_by_backend_ids, fn ids ->
      assert Repo.get_dynamic_repo() == Repo
      Mimic.call_original(Rules, :list_source_ids_by_backend_ids, [ids])
    end)

    expect(Rules, :list_source_ids_by_backend_ids, fn _ids ->
      raise DBConnection.ConnectionError, message: "primary unavailable"
    end)

    stub(ContextCache, :bust_keys, fn records ->
      result = Mimic.call_original(ContextCache, :bust_keys, [records])
      send(test_pid, {:busted, records})
      result
    end)

    change = %UpdatedRecord{
      relation: {"public", "backends"},
      record: %{"id" => Integer.to_string(backend.id)}
    }

    backend_id = backend.id
    expected = MapSet.new([{Rules, source_id: source.id}, {Rules, source_id: other.id}])

    for enabled <- [true, false] do
      Backends.get_backend(backend_id)
      |> Ecto.Changeset.change(enabled: enabled)
      |> Repo.update!()

      send(CacheBuster, %Transaction{changes: [change]})
      assert_receive {:busted, [{Backends, ^backend_id}]}, 1_000
      assert Cachex.get!(Backends.Cache, {:get_backend, [backend_id]}) == nil
      assert_receive {:busted, rule_records}, 2_000
      assert MapSet.new(rule_records) == expected
      assert length(rule_records) == 2

      for {drain_source, rule} <- [{source, first_rule}, {other, second_rule}] do
        event = build(:log_event, source: drain_source, message: "testing")
        tree = Rules.Cache.rules_tree_by_source_id(drain_source.id)
        assert rule.id in RulesTree.matching_rule_ids(event, tree) == enabled
      end

      assert {:cached, ^unrelated_tree} =
               Cachex.get!(Rules.Cache, {:rules_tree_by_source_id, [unrelated.id]})
    end
  end
end
