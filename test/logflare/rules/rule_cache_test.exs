defmodule Logflare.Rules.CacheTest do
  use Logflare.DataCase

  alias Logflare.ContextCache
  alias Logflare.Rules
  alias Logflare.Rules.Rule
  alias Logflare.Sources.SourceRouter.RulesTree

  @subject Rules.Cache

  setup do
    insert(:plan)
    user = insert(:user)
    backend = insert(:backend)
    source = insert(:source, user: user, log_events_updated_at: DateTime.utc_now())
    [r1, r2] = insert_list(2, :rule, source: source, backend: backend)

    [source: source, backend: backend, rule_ids: [r1.id, r2.id]]
  end

  describe "rules cache" do
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

      assert {:ok, 2} = ContextCache.bust_keys([{Rules, source_id: source.id}])
      assert [_r1, _r2] = @subject.list_rules(source)
      assert %{misses: 3, writes: 3} = Cachex.stats!(@subject)

      assert _ = @subject.rules_tree_by_source_id(source.id)
      assert %{misses: 4, writes: 4} = Cachex.stats!(@subject)
    end

    test "trees omit disabled destinations while raw lists retain them", %{
      source: source,
      rule_ids: rule_ids
    } do
      disabled = insert(:backend, enabled: false)
      disabled_rule = insert(:rule, source: source, backend: disabled)
      sink = insert(:source, user_id: source.user_id)
      source_rule = insert(:rule, source: source, sink: sink.token)
      event = build(:log_event, source: source, message: "testing")

      matching_ids =
        RulesTree.matching_rule_ids(event, @subject.rules_tree_by_source_id(source.id))

      assert MapSet.new(matching_ids) == MapSet.new([source_rule.id | rule_ids])
      assert Enum.any?(@subject.list_rules(source), &(&1.id == disabled_rule.id))
      assert [%{id: id}] = @subject.list_rules(disabled)
      assert id == disabled_rule.id
    end

    test "tree misses use the primary and hits avoid database reads", %{source: source} do
      expect(Rules, :rules_tree_by_source_id, fn id ->
        assert id == source.id
        assert Repo.get_dynamic_repo() == Repo
        Mimic.call_original(Rules, :rules_tree_by_source_id, [id])
      end)

      previous_repo = Repo.put_dynamic_repo(:unavailable_replica)
      previous_replicas = Application.fetch_env!(:logflare, :read_replicas)
      Application.put_env(:logflare, :read_replicas, [:unavailable_replica])

      try do
        assert [_entry | _rest] = tree = @subject.rules_tree_by_source_id(source.id)
        assert Repo.get_dynamic_repo() == :unavailable_replica
        reject(Rules, :rules_tree_by_source_id, 1)
        assert @subject.rules_tree_by_source_id(source.id) == tree
      after
        Repo.put_dynamic_repo(previous_repo)
        Application.put_env(:logflare, :read_replicas, previous_replicas)
      end
    end

    test "backend id key busting", %{backend: backend} do
      assert [_r1, _r2] = @subject.list_rules(backend)
      assert %{misses: 1, writes: 1} = Cachex.stats!(@subject)

      assert {:ok, 1} = ContextCache.bust_keys([{Rules, backend_id: backend.id}])
      assert [_r1, _r2] = @subject.list_rules(backend)
      assert %{misses: 2, writes: 2} = Cachex.stats!(@subject)
    end

    test "rule id key busting", %{rule_ids: [rid1, rid2]} do
      assert _r1 = @subject.get_rule(rid1)
      assert %{misses: 1, writes: 1} = Cachex.stats!(@subject)

      assert {:ok, 1} = ContextCache.bust_keys([{Rules, rid1}])
      assert _r1 = @subject.get_rule(rid1)
      assert %{misses: 2, writes: 2} = Cachex.stats!(@subject)

      # Bust missing key
      assert {:ok, 0} = ContextCache.bust_keys([{Rules, rid2}])
    end

    test "cache warming" do
      assert Cachex.warm!(@subject, wait: true) == [Logflare.Rules.CacheWarmer]
      assert Cachex.size!(@subject) == 1
    end
  end
end
