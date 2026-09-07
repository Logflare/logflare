defmodule Logflare.Rules.RoutingSnapshotTest do
  use ExUnit.Case, async: true

  alias Logflare.Rules.RoutingSnapshot
  alias Logflare.Rules.RoutingSnapshotStore

  setup do
    store = start_supervised!({RoutingSnapshotStore, name: nil})
    %{store: store}
  end

  test "sparse, dense and fallback reads preserve IDs, targets and order", %{store: store} do
    entries = entries(20)
    snapshot = RoutingSnapshot.new(1, entries, store: store)

    for ids <- [[], [1], [20, 1, 10], Enum.to_list(20..1//-1), [0, 21], [1, 1]] do
      assert RoutingSnapshot.resolve(snapshot, ids) == expected(entries, ids)
    end

    RoutingSnapshot.new(1, entries(20, 2), store: store)
    refute :ets.member(snapshot.table, snapshot.key)

    for ids <- [[], [1], [20, 1, 10], Enum.to_list(20..1//-1), [0, 21], [1, 1]] do
      assert RoutingSnapshot.resolve(snapshot, ids) == expected(entries, ids)
    end
  end

  test "empty snapshots and missing or nil targets are rejected", %{store: store} do
    empty = RoutingSnapshot.new(1, [], store: store)
    assert RoutingSnapshot.resolve(empty, [1]) == []

    entries = List.replace_at(entries(20), 0, {1, nil})
    snapshot = RoutingSnapshot.new(1, entries, store: store)
    assert RoutingSnapshot.resolve(snapshot, [1, 2, 30]) == [Map.new(entries)[2]]
    assert RoutingSnapshot.resolve(snapshot, Enum.to_list(1..30)) == expected(entries, 1..30)

    RoutingSnapshot.new(1, [], store: store)
    assert RoutingSnapshot.resolve(snapshot, [1, 2, 30]) == [Map.new(entries)[2]]
  end

  test "binary index supports zero, non-contiguous IDs and bigint boundaries", %{store: store} do
    ids = [0, 5, 91, 4_294_967_297, 9_223_372_036_854_775_807]
    entries = Enum.map(ids, &{&1, {&1, &1, nil}})
    snapshot = RoutingSnapshot.new(1, entries, store: store)

    for id <- ids do
      assert RoutingSnapshot.resolve(snapshot, [id]) == [Map.new(entries)[id]]
    end

    for id <- [1, 6, 90, 92, 4_294_967_296] do
      assert RoutingSnapshot.resolve(snapshot, [id]) == []
    end
  end

  test "ETS paths do not decode the fallback", %{store: store} do
    entries = entries(20)
    snapshot = %{RoutingSnapshot.new(1, entries, store: store) | encoded: <<>>}

    for count <- [1, 9, 10, 11, 20] do
      ids = Enum.to_list(count..1//-1)
      assert RoutingSnapshot.resolve(snapshot, ids) == expected(entries, ids)
    end
  end

  test "a decoded batch-local fallback avoids repeated decompression", %{store: store} do
    entries = entries(20)
    snapshot = RoutingSnapshot.new(1, entries, store: store)
    RoutingSnapshot.new(1, entries(20, 2), store: store)

    local =
      snapshot
      |> RoutingSnapshot.with_decoded(:erlang.binary_to_term(snapshot.encoded))
      |> Map.put(:encoded, <<>>)

    assert {:ok, expected(entries, [1, 20])} ==
             RoutingSnapshot.resolve_with_status(local, [1, 20])
  end

  test "header heap size does not grow with the target payload", %{store: store} do
    small = RoutingSnapshot.new(1, entries(20), store: store)
    large = RoutingSnapshot.new(2, entries(1000), store: store)
    assert :erts_debug.flat_size(small) == :erts_debug.flat_size(large)
    assert byte_size(large.encoded) > byte_size(small.encoded)
    assert large.estimated_bytes > small.estimated_bytes
  end

  test "suspended readers survive rebuilds without retaining ETS generations", %{store: store} do
    parent = self()
    entries = entries(20)
    snapshot = RoutingSnapshot.new(1, entries, store: store)

    reader =
      Task.async(fn ->
        send(parent, :acquired)
        receive do: (:resume -> RoutingSnapshot.resolve(snapshot, [1, 20]))
      end)

    assert_receive :acquired

    for generation <- 2..100 do
      RoutingSnapshot.new(1, entries(20, generation), store: store)
      assert_sizes(store, 1)
    end

    refute :ets.member(snapshot.table, snapshot.key)
    send(reader.pid, :resume)
    assert Task.await(reader) == expected(entries, [1, 20])
  end

  test "concurrent replacement never mixes generations", %{store: store} do
    1..8
    |> Task.async_stream(
      fn reader ->
        for generation <- 1..50 do
          entries = entries(20, {reader, generation})
          snapshot = RoutingSnapshot.new(1, entries, store: store)
          assert RoutingSnapshot.resolve(snapshot, [20, 1, 10]) == expected(entries, [20, 1, 10])

          assert RoutingSnapshot.resolve(snapshot, Enum.to_list(1..20)) ==
                   expected(entries, 1..20)
        end
      end,
      max_concurrency: 8
    )
    |> Enum.each(fn result -> assert {:ok, _} = result end)

    assert_sizes(store, 1)
  end

  test "a reader crash does not pin generations or require release", %{store: store} do
    parent = self()
    snapshot = RoutingSnapshot.new(1, entries(20), store: store)

    {pid, ref} =
      spawn_monitor(fn ->
        send(parent, {:acquired, RoutingSnapshot.resolve(snapshot, [1])})
        receive do: (:crash -> exit(:reader_crashed))
      end)

    assert_receive {:acquired, [{1, _backend_id, nil}]}
    send(pid, :crash)
    assert_receive {:DOWN, ^ref, :process, ^pid, :reader_crashed}
    RoutingSnapshot.new(1, entries(20, 2), store: store)
    refute :ets.member(snapshot.table, snapshot.key)
    assert_sizes(store, 1)
  end

  test "store restart cannot invalidate a reader's snapshot", %{store: store} do
    entries = entries(20)
    snapshot = RoutingSnapshot.new(1, entries, store: store)
    stop_supervised!(RoutingSnapshotStore)
    new_store = start_supervised!({RoutingSnapshotStore, name: nil})
    RoutingSnapshot.new(1, entries(20, 2), store: new_store)

    assert :ets.info(snapshot.table) == :undefined
    assert RoutingSnapshot.resolve(snapshot, [1]) == expected(entries, [1])
    assert RoutingSnapshot.resolve(snapshot, Enum.to_list(1..20)) == expected(entries, 1..20)
  end

  test "capacity eviction bounds all indexes and preserves acquired snapshots" do
    store = start_supervised!({RoutingSnapshotStore, name: nil, limit: 2}, id: :limited)
    entries = entries(20)
    snapshot = RoutingSnapshot.new(1, entries, store: store)

    for source_id <- 2..50, do: RoutingSnapshot.new(source_id, entries, store: store)

    assert_sizes(store, 2)
    refute :ets.member(snapshot.table, snapshot.key)
    assert RoutingSnapshot.resolve(snapshot, [1]) == expected(entries, [1])
  end

  test "estimated-byte eviction bounds the store while retaining one oversized snapshot" do
    sample_entries = entries(20)
    entry_tuple = List.to_tuple(sample_entries)
    rules_by_id = Map.new(sample_entries)
    encoded = :erlang.term_to_binary(rules_by_id, compressed: 1)
    weight = :erlang.external_size(entry_tuple) + 20 * 8 + byte_size(encoded)

    store =
      start_supervised!(
        {RoutingSnapshotStore, name: nil, max_bytes: weight + 1},
        id: :byte_limited
      )

    first = RoutingSnapshot.new(1, sample_entries, store: store)
    second = RoutingSnapshot.new(2, sample_entries, store: store)

    refute :ets.member(first.table, first.key)
    assert :ets.member(second.table, second.key)
    assert_sizes(store, 1)
    assert :sys.get_state(store).estimated_bytes == second.estimated_bytes

    oversized = RoutingSnapshot.new(3, entries(100), store: store)
    assert :ets.member(oversized.table, oversized.key)
    assert_sizes(store, 1)
    assert :sys.get_state(store).estimated_bytes == oversized.estimated_bytes
  end

  test "expiry retires all indexes without invalidating readers" do
    store = start_supervised!({RoutingSnapshotStore, name: nil, ttl: 0}, id: :expired)
    entries = entries(20)
    snapshot = RoutingSnapshot.new(1, entries, store: store)
    RoutingSnapshotStore.prune(store)

    assert_sizes(store, 0)
    assert :sys.get_state(store).estimated_bytes == 0
    assert RoutingSnapshot.resolve(snapshot, [1]) == expected(entries, [1])
  end

  test "late retirement cannot remove the replacement generation", %{store: store} do
    old = RoutingSnapshot.new(1, entries(20), store: store)
    current_entries = entries(20, 2)
    current = RoutingSnapshot.new(1, current_entries, store: store)
    RoutingSnapshotStore.delete(store, old.key)
    assert_sizes(store, 1)
    assert :ets.member(current.table, current.key)

    RoutingSnapshotStore.delete(store, current.key)
    assert_sizes(store, 0)
    assert RoutingSnapshot.resolve(current, [1]) == expected(current_entries, [1])
  end

  test "store telemetry reports source and estimated-byte gauges", %{store: store} do
    handler = "routing-snapshot-store-test-#{System.unique_integer([:positive])}"
    parent = self()

    :telemetry.attach(
      handler,
      [:logflare, :rules, :routing_snapshot_store],
      fn event, measurements, metadata, pid -> send(pid, {event, measurements, metadata}) end,
      parent
    )

    on_exit(fn -> :telemetry.detach(handler) end)
    snapshot = RoutingSnapshot.new(999_999, entries(20), store: store)

    assert_receive {
      [:logflare, :rules, :routing_snapshot_store],
      %{sources: 1, estimated_bytes: bytes},
      %{action: :put}
    }

    assert bytes == snapshot.estimated_bytes
  end

  defp entries(count, generation \\ 1) do
    generation = :erlang.phash2(generation)
    for id <- 1..count, do: {id, {id, generation * 10_000 + id, nil}}
  end

  defp expected(entries, ids) do
    rules_by_id = Map.new(entries)
    for id <- ids, target = Map.get(rules_by_id, id), do: target
  end

  defp assert_sizes(store, expected) do
    state = :sys.get_state(store)
    assert :ets.info(state.table, :size) == expected
    assert :ets.info(state.sources, :size) == expected
    assert :ets.info(state.expiry, :size) == expected
  end
end
