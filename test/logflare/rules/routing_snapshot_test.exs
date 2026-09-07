defmodule Logflare.Rules.RoutingSnapshotTest do
  use ExUnit.Case, async: true

  alias Logflare.Rules.Rule
  alias Logflare.Rules.RoutingSnapshot
  alias Logflare.Rules.RoutingSnapshotStore

  setup do
    store = start_supervised!({RoutingSnapshotStore, name: nil})
    %{store: store}
  end

  test "sparse, dense and fallback reads preserve IDs, values and order", %{store: store} do
    rules = rules(20)
    snapshot = RoutingSnapshot.new(1, rules, store)

    for ids <- [[], [1], [20, 1, 10], Enum.to_list(20..1//-1), [0, 21], [1, 1]] do
      assert RoutingSnapshot.resolve(snapshot, ids) == expected(rules, ids)
    end

    RoutingSnapshot.new(1, rules(20, 2), store)
    refute :ets.member(snapshot.table, snapshot.key)

    for ids <- [[], [1], [20, 1, 10], Enum.to_list(20..1//-1), [0, 21], [1, 1]] do
      assert RoutingSnapshot.resolve(snapshot, ids) == expected(rules, ids)
    end
  end

  test "empty snapshots and missing or nil rules are rejected", %{store: store} do
    empty = RoutingSnapshot.new(1, %{}, store)
    assert RoutingSnapshot.resolve(empty, [1]) == []

    rules = rules(20) |> Map.put(1, nil)
    snapshot = RoutingSnapshot.new(1, rules, store)
    assert RoutingSnapshot.resolve(snapshot, [1, 2, 30]) == [rules[2]]
    assert RoutingSnapshot.resolve(snapshot, Enum.to_list(1..30)) == expected(rules, 1..30)

    RoutingSnapshot.new(1, %{}, store)
    assert RoutingSnapshot.resolve(snapshot, [1, 2, 30]) == [rules[2]]
  end

  test "binary index supports zero, non-contiguous IDs and bigint boundaries", %{store: store} do
    ids = [0, 5, 91, 4_294_967_297, 9_223_372_036_854_775_807]
    rules = Map.new(ids, &{&1, %Rule{id: &1}})
    snapshot = RoutingSnapshot.new(1, rules, store)

    for id <- ids do
      assert RoutingSnapshot.resolve(snapshot, [id]) == [rules[id]]
    end

    for id <- [1, 6, 90, 92, 4_294_967_296] do
      assert RoutingSnapshot.resolve(snapshot, [id]) == []
    end
  end

  test "ETS paths do not decode the fallback", %{store: store} do
    rules = rules(20)
    snapshot = %{RoutingSnapshot.new(1, rules, store) | encoded: <<>>}

    for count <- [1, 9, 10, 11, 20] do
      ids = Enum.to_list(count..1//-1)
      assert RoutingSnapshot.resolve(snapshot, ids) == expected(rules, ids)
    end
  end

  test "header heap size does not grow with the rule payload", %{store: store} do
    small = RoutingSnapshot.new(1, rules(20), store)
    large = RoutingSnapshot.new(2, rules(1000), store)
    assert :erts_debug.flat_size(small) == :erts_debug.flat_size(large)
    assert byte_size(large.encoded) > byte_size(small.encoded)
  end

  test "suspended readers survive rebuilds without retaining ETS generations", %{store: store} do
    parent = self()
    rules = rules(20)
    snapshot = RoutingSnapshot.new(1, rules, store)

    reader =
      Task.async(fn ->
        send(parent, :acquired)
        receive do: (:resume -> RoutingSnapshot.resolve(snapshot, [1, 20]))
      end)

    assert_receive :acquired

    for generation <- 2..100 do
      RoutingSnapshot.new(1, rules(20, generation), store)
      assert_sizes(store, 1)
    end

    refute :ets.member(snapshot.table, snapshot.key)
    send(reader.pid, :resume)
    assert Task.await(reader) == expected(rules, [1, 20])
  end

  test "concurrent replacement never mixes generations", %{store: store} do
    1..8
    |> Task.async_stream(
      fn reader ->
        for generation <- 1..50 do
          rules = rules(20, {reader, generation})
          snapshot = RoutingSnapshot.new(1, rules, store)
          assert RoutingSnapshot.resolve(snapshot, [20, 1, 10]) == expected(rules, [20, 1, 10])
          assert RoutingSnapshot.resolve(snapshot, Enum.to_list(1..20)) == expected(rules, 1..20)
        end
      end,
      max_concurrency: 8
    )
    |> Enum.each(fn result -> assert {:ok, _} = result end)

    assert_sizes(store, 1)
  end

  test "a reader crash does not pin generations or require release", %{store: store} do
    parent = self()
    snapshot = RoutingSnapshot.new(1, rules(20), store)

    {pid, ref} =
      spawn_monitor(fn ->
        send(parent, {:acquired, RoutingSnapshot.resolve(snapshot, [1])})
        receive do: (:crash -> exit(:reader_crashed))
      end)

    assert_receive {:acquired, [%Rule{id: 1}]}
    send(pid, :crash)
    assert_receive {:DOWN, ^ref, :process, ^pid, :reader_crashed}
    RoutingSnapshot.new(1, rules(20, 2), store)
    refute :ets.member(snapshot.table, snapshot.key)
    assert_sizes(store, 1)
  end

  test "store restart cannot invalidate a reader's snapshot", %{store: store} do
    rules = rules(20)
    snapshot = RoutingSnapshot.new(1, rules, store)
    stop_supervised!(RoutingSnapshotStore)
    new_store = start_supervised!({RoutingSnapshotStore, name: nil})
    RoutingSnapshot.new(1, rules(20, 2), new_store)

    assert :ets.info(snapshot.table) == :undefined
    assert RoutingSnapshot.resolve(snapshot, [1]) == expected(rules, [1])
    assert RoutingSnapshot.resolve(snapshot, Enum.to_list(1..20)) == expected(rules, 1..20)
  end

  test "capacity eviction bounds all indexes and preserves acquired snapshots" do
    store = start_supervised!({RoutingSnapshotStore, name: nil, limit: 2}, id: :limited)
    rules = rules(20)
    snapshot = RoutingSnapshot.new(1, rules, store)

    for source_id <- 2..50, do: RoutingSnapshot.new(source_id, rules, store)

    assert_sizes(store, 2)
    refute :ets.member(snapshot.table, snapshot.key)
    assert RoutingSnapshot.resolve(snapshot, [1]) == expected(rules, [1])
  end

  test "expiry retires all indexes without invalidating readers" do
    store = start_supervised!({RoutingSnapshotStore, name: nil, ttl: 0}, id: :expired)
    rules = rules(20)
    snapshot = RoutingSnapshot.new(1, rules, store)
    RoutingSnapshotStore.prune(store)

    assert_sizes(store, 0)
    assert RoutingSnapshot.resolve(snapshot, [1]) == expected(rules, [1])
  end

  test "late retirement cannot remove the replacement generation", %{store: store} do
    old = RoutingSnapshot.new(1, rules(20), store)
    current = RoutingSnapshot.new(1, rules(20, 2), store)
    RoutingSnapshotStore.delete(store, old.key)
    assert_sizes(store, 1)
    assert :ets.member(current.table, current.key)

    RoutingSnapshotStore.delete(store, current.key)
    assert_sizes(store, 0)
    assert RoutingSnapshot.resolve(current, [1]) == expected(rules(20, 2), [1])
  end

  defp rules(count, generation \\ 1) do
    Map.new(1..count, &{&1, %Rule{id: &1, lql_string: inspect(generation)}})
  end

  defp expected(rules, ids), do: for(id <- ids, rule = rules[id], do: rule)

  defp assert_sizes(store, expected) do
    state = :sys.get_state(store)
    assert :ets.info(state.table, :size) == expected
    assert :ets.info(state.sources, :size) == expected
    assert :ets.info(state.expiry, :size) == expected
  end
end
