Mimic.copy(Cachex)

defmodule Logflare.Rules.CacheTest do
  alias Logflare.Rules.Rule
  use Logflare.DataCase

  alias Logflare.ContextCache.Supervisor, as: ContextCacheSupervisor
  alias Logflare.Rules
  alias Logflare.Rules.RoutingSnapshot
  alias Logflare.Rules.RoutingSnapshotStore
  alias Logflare.Sources
  alias Logflare.Sources.SourceRouter.Target

  @subject Rules.Cache

  setup do
    insert(:plan)
    user = insert(:user)
    backend = insert(:backend)
    source = insert(:source, user: user, log_events_updated_at: DateTime.utc_now())
    [r1, r2] = insert_list(2, :rule, source: source, backend: backend)

    on_exit(fn -> @subject.bust_by(source_id: source.id) end)

    [source: source, backend: backend, rule_ids: [r1.id, r2.id]]
  end

  describe "rules cache" do
    test "get rule", %{rule_ids: [rid1, _rid2]} do
      assert %Rule{id: ^rid1} = @subject.get_rule(rid1)

      assert Cachex.size!(@subject) == 1
      assert %{hits: 0, writes: 1} = Cachex.stats!(@subject)

      Mimic.reject(Rules, :get_rule, 1)

      assert %Rule{id: ^rid1} = @subject.get_rule(rid1)
      assert %{hits: 1, writes: 1} = Cachex.stats!(@subject)
    end

    test "get rules", %{rule_ids: rule_ids} do
      ref = :telemetry_test.attach_event_handlers(self(), [[:logflare, :repo, :replica_route]])
      on_exit(fn -> :telemetry.detach(ref) end)

      assert rules = @subject.get_rules(rule_ids)

      for _id <- rule_ids do
        assert_receive {[:logflare, :repo, :replica_route], ^ref, %{count: 1},
                        %{role: :primary, reason: :not_configured}}
      end

      for %Rule{id: id} <- rules do
        assert id in rule_ids
      end

      assert Cachex.size!(@subject) == 2
      assert %{hits: 0, writes: 2} = Cachex.stats!(@subject)
      Mimic.reject(Rules, :get_rule, 1)
      assert [_r1, _r2] = @subject.get_rules(rule_ids)
      assert %{hits: 2, writes: 2} = Cachex.stats!(@subject)
      [rid1, _rid2] = rule_ids
      assert %Rule{id: ^rid1} = @subject.get_rule(rid1)
      assert %{hits: 3, writes: 2} = Cachex.stats!(@subject)
    end

    test "rules tree by source id caches the tree with compact targets", %{
      source: source,
      rule_ids: rule_ids
    } do
      assert {tree, %RoutingSnapshot{} = snapshot} = @subject.rules_tree_by_source_id(source.id)
      positions = Enum.to_list(0..(snapshot.count - 1))
      assert snapshot.count == length(rule_ids)
      assert Enum.map(RoutingSnapshot.resolve(snapshot, positions), &Target.id/1) == rule_ids

      Mimic.reject(Rules, :rules_tree_by_source_id, 1)
      assert @subject.rules_tree_by_source_id(source.id) == {tree, snapshot}
    end

    test "rules tree cache misses recover through fallback while the store is unavailable", %{
      source: source,
      rule_ids: rule_ids
    } do
      on_exit(fn ->
        if Process.whereis(RoutingSnapshotStore) == nil do
          Supervisor.restart_child(ContextCacheSupervisor, RoutingSnapshotStore)
        end
      end)

      assert :ok = Supervisor.terminate_child(ContextCacheSupervisor, RoutingSnapshotStore)

      assert {tree, %RoutingSnapshot{table: RoutingSnapshotStore} = snapshot} =
               @subject.rules_tree_by_source_id(source.id)

      positions = Enum.to_list(0..(snapshot.count - 1))
      expected = RoutingSnapshot.resolve(snapshot, positions)
      assert Enum.map(expected, &Target.id/1) == rule_ids
      assert @subject.rules_tree_by_source_id(source.id) == {tree, snapshot}

      assert {:ok, _pid} = Supervisor.restart_child(ContextCacheSupervisor, RoutingSnapshotStore)

      assert {:fallback, ^expected, _decoded} =
               RoutingSnapshot.resolve_with_status(snapshot, positions)

      assert :ok = RoutingSnapshot.restore(snapshot)
      :sys.get_state(RoutingSnapshotStore)
      assert {:ok, ^expected} = RoutingSnapshot.resolve_with_status(snapshot, positions)
      assert {^tree, ^snapshot} = @subject.rules_tree_by_source_id(source.id)
    end

    for invalidation <- [:bust, :expire, :clear] do
      @invalidation invalidation
      test "#{invalidation} and rebuild preserve a paused reader's snapshot", %{
        source: source
      } do
        {tree, old} = @subject.rules_tree_by_source_id(source.id)
        positions = Enum.to_list(0..(old.count - 1))
        old_targets = RoutingSnapshot.resolve(old, positions)
        parent = self()

        reader =
          Task.async(fn ->
            send(parent, :snapshot_acquired)
            receive do: (:resume -> RoutingSnapshot.resolve(old, positions))
          end)

        assert_receive :snapshot_acquired

        case @invalidation do
          :bust ->
            assert {:ok, 1} = @subject.bust_by(source_id: source.id)

          :expire ->
            assert {:ok, true} =
                     Cachex.expire(@subject, {:rules_tree_by_source_id, [source.id]}, -1)

          :clear ->
            assert {:ok, 1} = Cachex.clear(@subject)
        end

        new_targets =
          Enum.map(old_targets, fn {id, backend_id, sink} ->
            {id, backend_id + 1_000_000, sink}
          end)

        expect(Rules, :rules_tree_by_source_id, fn id ->
          assert id == source.id
          {tree, new_targets}
        end)

        {^tree, current} = @subject.rules_tree_by_source_id(source.id)
        assert current.key != old.key
        assert RoutingSnapshot.resolve(current, positions) == new_targets
        refute :ets.member(old.table, old.key)
        send(reader.pid, :resume)
        assert Task.await(reader) == old_targets
      end
    end

    test "restores a missing generation without changing the cached header or TTL", %{
      source: source
    } do
      {tree, snapshot} = @subject.rules_tree_by_source_id(source.id)
      positions = Enum.to_list(0..(snapshot.count - 1))
      expected = RoutingSnapshot.resolve(snapshot, positions)
      key = {:rules_tree_by_source_id, [source.id]}
      {:ok, entry_before} = Cachex.inspect(@subject, {:entry, key})
      RoutingSnapshotStore.delete(RoutingSnapshotStore, snapshot.key)
      :sys.get_state(RoutingSnapshotStore)

      assert {:fallback, ^expected, _decoded} =
               RoutingSnapshot.resolve_with_status(snapshot, positions)

      assert :ok = RoutingSnapshot.restore(snapshot)
      :sys.get_state(RoutingSnapshotStore)
      assert {^tree, ^snapshot} = @subject.rules_tree_by_source_id(source.id)
      assert {:ok, ^entry_before} = Cachex.inspect(@subject, {:entry, key})
      assert {:ok, ^expected} = RoutingSnapshot.resolve_with_status(snapshot, positions)
    end

    test "store outage does not block restore or invalidation", %{source: source} do
      {_tree, snapshot} = @subject.rules_tree_by_source_id(source.id)

      on_exit(fn ->
        if Process.whereis(RoutingSnapshotStore) == nil do
          Supervisor.restart_child(ContextCacheSupervisor, RoutingSnapshotStore)
        end
      end)

      assert :ok = Supervisor.terminate_child(ContextCacheSupervisor, RoutingSnapshotStore)
      assert :ok = RoutingSnapshot.restore(snapshot)
      assert {:ok, 1} = @subject.bust_by(source_id: source.id)
      assert {:ok, _pid} = Supervisor.restart_child(ContextCacheSupervisor, RoutingSnapshotStore)
      assert :ok = RoutingSnapshot.restore(snapshot)
      :sys.get_state(RoutingSnapshotStore)
      assert Cachex.get(@subject, {:rules_tree_by_source_id, [source.id]}) == {:ok, nil}
    end

    test "stale repair cannot overwrite a newer cached generation", %{source: source} do
      {tree, old} = @subject.rules_tree_by_source_id(source.id)
      old_targets = :erlang.binary_to_term(old.encoded)

      new_targets =
        old_targets
        |> Tuple.to_list()
        |> Enum.map(fn {id, backend_id, sink} -> {id, backend_id + 1_000_000, sink} end)

      current = RoutingSnapshot.new(source.id, new_targets)
      cache_key = {:rules_tree_by_source_id, [source.id]}
      assert {:ok, true} = Cachex.put(@subject, cache_key, {:cached, {tree, current}})
      positions = Enum.to_list(0..(old.count - 1))

      assert {:fallback, _targets, ^old_targets} =
               RoutingSnapshot.resolve_with_status(old, positions)

      assert :ok = RoutingSnapshot.restore(old)
      :sys.get_state(RoutingSnapshotStore)
      assert {^tree, ^current} = @subject.rules_tree_by_source_id(source.id)
      assert :ets.member(current.table, current.key)
    end

    test "list by source", %{source: source, rule_ids: expected_rule_ids} do
      assert rules = @subject.list_by_source_id(source.id)

      for %Rule{id: id} <- rules do
        assert id in expected_rule_ids
      end

      assert Cachex.size(@subject) == {:ok, 1}
      assert %{hits: 0, writes: 1} = Cachex.stats!(@subject)

      Mimic.reject(Rules, :list_by_source_id, 1)

      assert [_r1, _r2] = @subject.list_by_source_id(source.id)
      assert %{hits: 1} = Cachex.stats!(@subject)

      assert [_r1, _r2] = @subject.list_rules(source)
      assert %{hits: 2} = Cachex.stats!(@subject)
    end

    test "is used on source preload", %{source: source} do
      assert [_r1, _r2] = @subject.list_by_source_id(source.id)
      assert Cachex.size(@subject) == {:ok, 1}
      assert %{hits: 0, writes: 1} = Cachex.stats!(@subject)

      source = Ecto.reset_fields(source, [:rules])
      Mimic.reject(Rules, :list_by_source_id, 1)

      assert Sources.Cache.preload_rules(source)
      assert %{hits: 1} = Cachex.stats!(@subject)
    end

    test "list by backend", %{backend: backend, rule_ids: expected_rule_ids} do
      assert rules = @subject.list_by_backend_id(backend.id)

      for %Rule{id: id} <- rules do
        assert id in expected_rule_ids
      end

      assert Cachex.size(@subject) == {:ok, 1}
      assert %{hits: 0, writes: 1} = Cachex.stats!(@subject)

      Mimic.reject(Rules, :list_by_backend_id, 1)

      assert [_r1, _r2] = @subject.list_by_backend_id(backend.id)
      assert %{hits: 1} = Cachex.stats!(@subject)

      assert [_r1, _r2] = @subject.list_rules(backend)
      assert %{hits: 2} = Cachex.stats!(@subject)
    end

    test "source id key busting", %{source: source} do
      assert [_r1, _r2] = @subject.list_rules(source)
      assert _ = @subject.rules_tree_by_source_id(source.id)
      assert %{misses: 2, writes: 2} = Cachex.stats!(@subject)

      assert {:ok, 2} = @subject.bust_by(source_id: source.id)
      assert [_r1, _r2] = @subject.list_rules(source)
      assert %{misses: 3, writes: 3} = Cachex.stats!(@subject)

      assert _ = @subject.rules_tree_by_source_id(source.id)
      assert %{misses: 4, writes: 4} = Cachex.stats!(@subject)
    end

    test "backend id key busting", %{backend: backend} do
      assert [_r1, _r2] = @subject.list_rules(backend)
      assert %{misses: 1, writes: 1} = Cachex.stats!(@subject)

      assert {:ok, 1} = @subject.bust_by(backend_id: backend.id)
      assert [_r1, _r2] = @subject.list_rules(backend)
      assert %{misses: 2, writes: 2} = Cachex.stats!(@subject)
    end

    test "rule id key busting", %{rule_ids: [rid1, rid2]} do
      assert _r1 = @subject.get_rule(rid1)
      assert %{misses: 1, writes: 1} = Cachex.stats!(@subject)

      assert {:ok, 1} = @subject.bust_by(id: rid1)
      assert _r1 = @subject.get_rule(rid1)
      assert %{misses: 4, writes: 2} = Cachex.stats!(@subject)

      # Bust missing key
      assert {:ok, 0} = @subject.bust_by(id: rid2)
    end

    test "ID-only invalidation refreshes routing destinations", %{
      source: source,
      backend: backend,
      rule_ids: [id, _other]
    } do
      {_tree, old} = @subject.rules_tree_by_source_id(source.id)
      assert @subject.get_rule(id).backend_id == backend.id
      replacement = insert(:backend)
      rule = Repo.get!(Rule, id)
      assert {:ok, _rule} = Rules.update_rule(rule, %{backend_id: replacement.id})

      assert {:ok, 2} = @subject.bust_by(id: id)
      assert @subject.get_rule(id).backend_id == replacement.id
      {_tree, current} = @subject.rules_tree_by_source_id(source.id)
      assert resolve_ids(current, [id]) == [{id, replacement.id, nil}]
      assert resolve_ids(old, [id]) == [{id, backend.id, nil}]
    end

    test "ID-only invalidation finds deleted rules in snapshots without per-rule entries", %{
      source: source,
      rule_ids: [id, _other]
    } do
      {_tree, _old} = @subject.rules_tree_by_source_id(source.id)
      Repo.delete!(Repo.get!(Rule, id))

      assert {:ok, 1} = @subject.bust_by(id: id)
      {_tree, current} = @subject.rules_tree_by_source_id(source.id)
      assert resolve_ids(current, [id]) == []
    end

    test "ID-only invalidation finds the owner of a newly inserted rule", %{source: source} do
      {_tree, _old} = @subject.rules_tree_by_source_id(source.id)
      rule = insert(:rule, source: source, backend: insert(:backend))

      assert {:ok, 1} = @subject.bust_by(id: rule.id)
      {_tree, current} = @subject.rules_tree_by_source_id(source.id)
      assert resolve_ids(current, [rule.id]) == [Target.from_rule(rule)]
    end

    test "generic primary-key invalidation retires derived routing snapshots", %{
      source: source,
      rule_ids: [id, _other]
    } do
      {_tree, _old} = @subject.rules_tree_by_source_id(source.id)
      replacement = insert(:backend)
      assert {:ok, _rule} = Rules.update_rule(Repo.get!(Rule, id), %{backend_id: replacement.id})

      assert {:ok, 1} = Logflare.ContextCache.bust_keys([{Rules, id}])
      {_tree, current} = @subject.rules_tree_by_source_id(source.id)
      assert resolve_ids(current, [id]) == [{id, replacement.id, nil}]
    end

    test "ID-only invalidation retires both owners after a move", %{
      source: source,
      backend: backend,
      rule_ids: [id, _other]
    } do
      destination = insert(:source, user: source.user)
      {_tree, _old} = @subject.rules_tree_by_source_id(source.id)
      {_tree, _empty} = @subject.rules_tree_by_source_id(destination.id)
      Repo.update_all(from(rule in Rule, where: rule.id == ^id), set: [source_id: destination.id])

      assert {:ok, 2} = @subject.bust_by(id: id)
      {_tree, old_owner} = @subject.rules_tree_by_source_id(source.id)
      {_tree, new_owner} = @subject.rules_tree_by_source_id(destination.id)
      assert resolve_ids(old_owner, [id]) == []
      assert resolve_ids(new_owner, [id]) == [{id, backend.id, nil}]
    end

    test "source-aware invalidation does not scan headers or query rule ownership", %{
      source: source,
      rule_ids: [id, _other]
    } do
      {_tree, _snapshot} = @subject.rules_tree_by_source_id(source.id)
      Mimic.reject(Cachex, :stream, 3)
      ref = :telemetry_test.attach_event_handlers(self(), [[:logflare, :repo, :query]])
      on_exit(fn -> :telemetry.detach(ref) end)

      assert {:ok, 1} = @subject.bust_by(id: id, source_id: source.id, source_id: source.id)
      refute_receive {[:logflare, :repo, :query], ^ref, _measurements, _metadata}
    end

    test "cache warming" do
      assert Cachex.warm!(@subject, wait: true) == [Logflare.Rules.CacheWarmer]
      assert Cachex.size!(@subject) == 1
    end
  end

  defp resolve_ids(snapshot, ids) do
    positions =
      snapshot.encoded
      |> :erlang.binary_to_term()
      |> Tuple.to_list()
      |> Enum.with_index()
      |> Enum.filter(fn {target, _position} -> is_tuple(target) and Target.id(target) in ids end)
      |> Enum.map(&elem(&1, 1))

    RoutingSnapshot.resolve(snapshot, positions)
  end
end
