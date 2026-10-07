Mimic.copy(Cachex)
Mimic.copy(Logflare.Rules.RoutingSnapshotStore)

defmodule Logflare.Rules.RoutingCacheConcurrencyTest do
  use Logflare.DataCase

  require Cachex.Spec

  alias Logflare.Rules
  alias Logflare.Rules.Cache
  alias Logflare.Rules.RoutingSnapshot
  alias Logflare.Rules.RoutingSnapshotStore

  test "restore and invalidation do not wait for a suspended store or enable transactions" do
    source_id = 1_900_090_003
    key = {:rules_tree_by_source_id, [source_id]}
    snapshot = RoutingSnapshot.new(source_id, snapshot_entries([{1, 10, nil}]))
    Cachex.put!(Cache, key, {:cached, {[], snapshot}})
    RoutingSnapshotStore.delete(RoutingSnapshotStore, snapshot.key)
    :sys.get_state(RoutingSnapshotStore)
    cleanup_headers([source_id])

    :sys.suspend(RoutingSnapshotStore)

    try do
      reader = Task.async(fn -> RoutingSnapshot.restore(snapshot) end)
      assert Task.await(reader) == :ok
      bust = Task.async(fn -> Cache.bust_by(source_id: source_id) end)
      assert Task.await(bust) == {:ok, 1}
      assert Cachex.get(Cache, key) == {:ok, nil}
    after
      :sys.resume(RoutingSnapshotStore)
    end

    :sys.get_state(RoutingSnapshotStore)
    assert Cachex.get(Cache, key) == {:ok, nil}
    assert {:ok, cache} = Cachex.inspect(Cache, :cache)
    refute Cachex.Spec.cache(cache, :transactions)
    assert RoutingSnapshot.resolve(snapshot, [1]) == [{1, 10, nil}]
  end

  test "a cold loader evicted before Cachex commits leaves a usable fallback header" do
    store = start_supervised!({RoutingSnapshotStore, name: nil, table: nil, limit: 1})
    parent = self()
    first_id = 1_900_090_101
    second_id = first_id + 1
    cleanup_headers([first_id, second_id])

    stub(Rules, :rules_tree_by_source_id, fn id ->
      {[], snapshot_entries([{id, 10, nil}])}
    end)

    stub(RoutingSnapshotStore, :put, fn server, {id, _generation} = key, targets, bytes ->
      if id in [first_id, second_id] do
        table = Mimic.call_original(RoutingSnapshotStore, :put, [store, key, targets, bytes])

        if id == first_id do
          send(parent, {:registered, self(), table, key})
          receive do: (:publish -> :ok)
        end

        table
      else
        Mimic.call_original(RoutingSnapshotStore, :put, [server, key, targets, bytes])
      end
    end)

    reader = Task.async(fn -> Cache.rules_tree_by_source_id(first_id) end)
    assert_receive {:registered, publisher, table, key}
    {_tree, current} = Cache.rules_tree_by_source_id(second_id)
    refute :ets.member(table, key)
    send(publisher, :publish)
    assert {_tree, old} = Task.await(reader)
    assert old.key == key
    assert RoutingSnapshot.resolve(old, [first_id]) == [{first_id, 10, nil}]
    assert Cachex.get!(Cache, {:rules_tree_by_source_id, [first_id]}) == {:cached, {[], old}}

    RoutingSnapshot.restore(old, store)
    state = :sys.get_state(store)
    refute Map.has_key?(state, :publishers)
    assert state.estimated_bytes == current.estimated_bytes
    assert :ets.info(state.sources, :size) == 1
    assert :ets.member(table, current.key)
    refute :ets.member(table, old.key)
  end

  for published? <- [false, true] do
    @published? published?
    test "publisher death #{if published?, do: "after", else: "before"} commit leaves only disposable acceleration" do
      store = start_supervised!({RoutingSnapshotStore, name: nil, table: nil})
      parent = self()
      source_id = 1_900_090_103
      cleanup_headers([source_id])

      {publisher, monitor} =
        spawn_monitor(fn ->
          snapshot =
            RoutingSnapshot.new(source_id, snapshot_entries([{1, 10, nil}]), store: store)

          if @published?,
            do:
              Cachex.put!(
                Cache,
                {:rules_tree_by_source_id, [source_id]},
                {:cached, {[], snapshot}}
              )

          send(parent, {:ready, snapshot})
          receive do: (:stop -> :ok)
        end)

      assert_receive {:ready, snapshot}
      Process.exit(publisher, :kill)
      assert_receive {:DOWN, ^monitor, :process, ^publisher, :killed}
      state = :sys.get_state(store)
      refute Map.has_key?(state, :publishers)
      assert state.estimated_bytes == snapshot.estimated_bytes
      assert :ets.member(snapshot.table, snapshot.key)

      expected = if @published?, do: {:cached, {[], snapshot}}, else: nil
      assert Cachex.get!(Cache, {:rules_tree_by_source_id, [source_id]}) == expected
      RoutingSnapshotStore.delete(store, snapshot.key)
      :sys.get_state(store)
      refute :ets.member(snapshot.table, snapshot.key)
      assert Cachex.get!(Cache, {:rules_tree_by_source_id, [source_id]}) == expected
      assert RoutingSnapshot.resolve(snapshot, [1]) == [{1, 10, nil}]
    end
  end

  test "delayed restore cannot replace a newer store row or cached header" do
    source_id = 1_900_090_105
    cleanup_headers([source_id])
    old = RoutingSnapshot.new(source_id, snapshot_entries([{1, 10, nil}]))
    RoutingSnapshotStore.delete(RoutingSnapshotStore, old.key)
    :sys.get_state(RoutingSnapshotStore)
    current = RoutingSnapshot.new(source_id, snapshot_entries([{1, 20, nil}]))
    cache_key = {:rules_tree_by_source_id, [source_id]}
    Cachex.put!(Cache, cache_key, {:cached, {[], current}})

    assert :ok = RoutingSnapshot.restore(old)
    :sys.get_state(RoutingSnapshotStore)
    assert Cachex.get!(Cache, cache_key) == {:cached, {[], current}}
    assert RoutingSnapshot.resolve(current, [1]) == [{1, 20, nil}]
    assert RoutingSnapshot.resolve(old, [1]) == [{1, 10, nil}]
    refute :ets.member(old.table, old.key)
  end

  test "capacity eviction never retires a completed header" do
    store = start_supervised!({RoutingSnapshotStore, name: nil, table: nil, limit: 1})
    source_id = 1_900_090_107
    cleanup_headers([source_id])
    snapshot = RoutingSnapshot.new(source_id, snapshot_entries([{1, 10, nil}]), store: store)
    key = {:rules_tree_by_source_id, [source_id]}
    Cachex.put!(Cache, key, {:cached, {[], snapshot}})
    current = RoutingSnapshot.new(source_id + 1, [], store: store)

    assert Cachex.get!(Cache, key) == {:cached, {[], snapshot}}
    refute :ets.member(snapshot.table, snapshot.key)
    assert RoutingSnapshot.resolve(snapshot, [1]) == [{1, 10, nil}]

    RoutingSnapshotStore.delete(store, current.key)
    :sys.get_state(store)
    RoutingSnapshot.restore(snapshot, store)
    :sys.get_state(store)
    assert :ets.member(snapshot.table, snapshot.key)
    assert Cachex.get!(Cache, key) == {:cached, {[], snapshot}}
  end

  defp cleanup_headers(ids) do
    on_exit(fn -> Enum.each(ids, &Cache.bust_by(source_id: &1)) end)
  end

  defp snapshot_entries(targets), do: Enum.map(targets, &{elem(&1, 0), &1})
end
