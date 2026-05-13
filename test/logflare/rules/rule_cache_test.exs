defmodule Logflare.Rules.CacheTest do
  alias Logflare.Rules.Rule
  use Logflare.DataCase

  alias Logflare.ContextCache
  alias Logflare.Rules
  alias Logflare.Sources

  @subject Rules.Cache

  setup do
    insert(:plan)
    user = insert(:user)
    backend = insert(:backend)
    source = insert(:source, user: user, log_events_updated_at: DateTime.utc_now())
    [r1, r2] = insert_list(2, :rule, source: source, backend: backend)

    [source: source, backend: backend, rule_ids: [r1.id, r2.id]]
  end

  describe "Rules.Cache" do
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
  end

  describe "Rules.Cache key busting" do
    test "by source id", %{source: source} do
      assert [_r1, _r2] = @subject.list_rules(source)
      assert _ = @subject.rules_tree_by_source_id(source.id)
      assert %{misses: 2, writes: 2} = Cachex.stats!(@subject)

      kw = [source_id: source.id]
      actions = @subject.bust_actions(:update, kw)
      assert :ok = ContextCache.refresh_keys([{Rules, kw, actions}])

      assert [_r1, _r2] = @subject.list_rules(source)
      assert %{misses: 3} = Cachex.stats!(@subject)

      assert _ = @subject.rules_tree_by_source_id(source.id)
      assert %{misses: 4} = Cachex.stats!(@subject)
    end

    test "by backend id", %{backend: backend} do
      assert [_r1, _r2] = @subject.list_rules(backend)
      assert %{misses: 1, writes: 1} = Cachex.stats!(@subject)

      kw = [backend_id: backend.id]
      actions = @subject.bust_actions(:update, kw)
      assert :ok = ContextCache.refresh_keys([{Rules, kw, actions}])

      assert [_r1, _r2] = @subject.list_rules(backend)
      assert %{misses: 2} = Cachex.stats!(@subject)
    end
  end

  describe "Rules.Cache key refreshing" do
    test "by rule id", %{rule_ids: [rid1, rid2]} do
      assert _r1 = @subject.get_rule(rid1)
      assert %{misses: 1, writes: 1} = Cachex.stats!(@subject)

      kw = [id: rid1]
      assert :ok = ContextCache.refresh_keys([{Rules, kw, @subject.bust_actions(:update, kw)}])
      assert _r1 = @subject.get_rule(rid1)
      assert %{hits: 1, misses: 1} = Cachex.stats!(@subject)

      # Refresh missing key
      size_before = Cachex.size!(@subject)
      kw = [id: rid2]
      assert :ok = ContextCache.refresh_keys([{Rules, kw, @subject.bust_actions(:update, kw)}])
      assert Cachex.size!(@subject) == size_before
    end
  end

  test "cache warming" do
    assert Cachex.warm!(@subject, wait: true) == [Logflare.Rules.CacheWarmer]
    assert Cachex.size!(@subject) == 1
  end
end
