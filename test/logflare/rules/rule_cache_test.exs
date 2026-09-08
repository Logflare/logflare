defmodule Logflare.Rules.CacheTest do
  alias Logflare.Rules.Rule
  use Logflare.DataCase

  alias Logflare.Rules
  alias Logflare.Rules.RoutingSnapshot
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
      assert rules = @subject.get_rules(rule_ids)

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

    test "repairs the still-current header after its ETS generation is lost", %{source: source} do
      {_tree, snapshot} = @subject.rules_tree_by_source_id(source.id)
      positions = Enum.to_list(0..(snapshot.count - 1))
      expected = RoutingSnapshot.resolve(snapshot, positions)

      _replacement =
        RoutingSnapshot.rehydrate(
          snapshot,
          source.id,
          :erlang.binary_to_term(snapshot.encoded)
        )

      assert {:fallback, ^expected, encoded_targets} =
               RoutingSnapshot.resolve_with_status(snapshot, positions)

      assert {:repaired, repaired} =
               @subject.repair_routing_snapshot(source.id, snapshot, encoded_targets)

      {_tree, ^repaired} = @subject.rules_tree_by_source_id(source.id)
      assert repaired.key != snapshot.key
      assert {:ok, ^expected} = RoutingSnapshot.resolve_with_status(repaired, positions)
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
      assert %{misses: 2, writes: 2} = Cachex.stats!(@subject)

      # Bust missing key
      assert {:ok, 0} = @subject.bust_by(id: rid2)
    end

    test "cache warming" do
      assert Cachex.warm!(@subject, wait: true) == [Logflare.Rules.CacheWarmer]
      assert Cachex.size!(@subject) == 1
    end
  end
end
