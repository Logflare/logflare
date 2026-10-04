defmodule Logflare.Rules.RoutingSnapshotTest do
  use ExUnit.Case, async: true

  alias Logflare.Rules.RoutingSnapshot
  alias Logflare.Rules.RoutingSnapshotStore

  setup do
    store = start_supervised!({RoutingSnapshotStore, name: nil})
    %{store: store}
  end

  test "sparse, dense and fallback reads preserve targets and order", %{store: store} do
    targets = targets(20)
    snapshot = RoutingSnapshot.new(1, targets, store: store)

    for positions <- [[], [0], [19, 0, 9], Enum.to_list(19..0//-1), [-1, 20], [0, 0]] do
      assert RoutingSnapshot.resolve(snapshot, positions) == expected(targets, positions)
    end

    RoutingSnapshot.new(1, targets(20, 2), store: store)
    refute :ets.member(snapshot.table, snapshot.key)

    for positions <- [[], [0], [19, 0, 9], Enum.to_list(19..0//-1), [-1, 20], [0, 0]] do
      assert RoutingSnapshot.resolve(snapshot, positions) == expected(targets, positions)
    end
  end

  test "empty snapshots and missing or nil positions are rejected", %{store: store} do
    empty = RoutingSnapshot.new(1, [], store: store)
    assert RoutingSnapshot.resolve(empty, [0]) == []

    targets = List.replace_at(targets(20), 0, nil)
    snapshot = RoutingSnapshot.new(1, targets, store: store)
    assert RoutingSnapshot.resolve(snapshot, [-1, 0, 1, 30]) == [Enum.at(targets, 1)]
    assert RoutingSnapshot.resolve(snapshot, Enum.to_list(0..29)) == expected(targets, 0..29)

    RoutingSnapshot.new(1, [], store: store)
    assert RoutingSnapshot.resolve(snapshot, [-1, 0, 1, 30]) == [Enum.at(targets, 1)]
  end

  test "position lookup supports boundaries without an ID index", %{store: store} do
    targets = targets(5)
    snapshot = RoutingSnapshot.new(1, targets, store: store)

    assert RoutingSnapshot.resolve(snapshot, [0, 4]) == [hd(targets), List.last(targets)]
    assert RoutingSnapshot.resolve(snapshot, [-1, 5, 4_294_967_297]) == []
    refute Map.has_key?(snapshot, :index)
  end

  test "ETS paths do not decode the fallback", %{store: store} do
    targets = targets(20)
    snapshot = %{RoutingSnapshot.new(1, targets, store: store) | encoded: <<>>}

    for count <- [1, 9, 10, 11, 20] do
      positions = Enum.to_list((count - 1)..0//-1)
      assert RoutingSnapshot.resolve(snapshot, positions) == expected(targets, positions)
    end
  end

  test "a decoded batch-local fallback avoids repeated decompression", %{store: store} do
    targets = targets(20)
    snapshot = RoutingSnapshot.new(1, targets, store: store)
    RoutingSnapshot.new(1, targets(20, 2), store: store)

    local =
      snapshot
      |> RoutingSnapshot.with_decoded(:erlang.binary_to_term(snapshot.encoded))
      |> Map.put(:encoded, <<>>)

    assert {:ok, expected(targets, [0, 19])} ==
             RoutingSnapshot.resolve_with_status(local, [0, 19])
  end

  test "header heap size does not grow with the target payload", %{store: store} do
    small = RoutingSnapshot.new(1, targets(20), store: store)
    large = RoutingSnapshot.new(2, targets(1000), store: store)
    assert :erts_debug.flat_size(small) == :erts_debug.flat_size(large)
    assert byte_size(large.encoded) > byte_size(small.encoded)
    assert large.estimated_bytes > small.estimated_bytes
  end

  test "suspended readers survive rebuilds without retaining ETS generations", %{store: store} do
    parent = self()
    targets = targets(20)
    snapshot = RoutingSnapshot.new(1, targets, store: store)

    reader =
      Task.async(fn ->
        send(parent, :acquired)
        receive do: (:resume -> RoutingSnapshot.resolve(snapshot, [0, 19]))
      end)

    assert_receive :acquired

    for generation <- 2..100 do
      RoutingSnapshot.new(1, targets(20, generation), store: store)
      assert_sizes(store, 1)
    end

    refute :ets.member(snapshot.table, snapshot.key)
    send(reader.pid, :resume)
    assert Task.await(reader) == expected(targets, [0, 19])
  end

  test "concurrent replacement never mixes generations", %{store: store} do
    1..8
    |> Task.async_stream(
      fn reader ->
        for generation <- 1..50 do
          targets = targets(20, {reader, generation})
          snapshot = RoutingSnapshot.new(1, targets, store: store)
          assert RoutingSnapshot.resolve(snapshot, [19, 0, 9]) == expected(targets, [19, 0, 9])

          assert RoutingSnapshot.resolve(snapshot, Enum.to_list(0..19)) ==
                   expected(targets, 0..19)
        end
      end,
      max_concurrency: 8
    )
    |> Enum.each(fn result -> assert {:ok, _} = result end)

    assert_sizes(store, 1)
  end

  test "a reader crash does not pin generations or require release", %{store: store} do
    parent = self()
    snapshot = RoutingSnapshot.new(1, targets(20), store: store)

    {pid, ref} =
      spawn_monitor(fn ->
        send(parent, {:acquired, RoutingSnapshot.resolve(snapshot, [0])})
        receive do: (:crash -> exit(:reader_crashed))
      end)

    assert_receive {:acquired, [{1, _backend_id, nil}]}
    send(pid, :crash)
    assert_receive {:DOWN, ^ref, :process, ^pid, :reader_crashed}
    RoutingSnapshot.new(1, targets(20, 2), store: store)
    refute :ets.member(snapshot.table, snapshot.key)
    assert_sizes(store, 1)
  end

  test "store outage preserves acquired readers and cold snapshots", %{store: store} do
    targets = targets(20)
    snapshot = RoutingSnapshot.new(1, targets, store: store)
    stop_supervised!(RoutingSnapshotStore)

    cold_targets = targets(20, 2)
    cold = RoutingSnapshot.new(2, cold_targets, store: store)

    assert cold.table == nil
    expected_cold_target = expected(cold_targets, [0])

    assert {:fallback, ^expected_cold_target, _target_tuple} =
             RoutingSnapshot.resolve_with_status(cold, [0])

    assert :ets.info(snapshot.table) == :undefined
    assert RoutingSnapshot.resolve(snapshot, [0]) == expected(targets, [0])
    assert RoutingSnapshot.resolve(snapshot, Enum.to_list(0..19)) == expected(targets, 0..19)

    new_store = start_supervised!({RoutingSnapshotStore, name: nil})

    repaired =
      RoutingSnapshot.rehydrate(
        cold,
        2,
        :erlang.binary_to_term(cold.encoded),
        new_store
      )

    assert {:ok, expected(cold_targets, [0])} ==
             RoutingSnapshot.resolve_with_status(repaired, [0])
  end

  test "capacity eviction bounds all indexes and preserves acquired snapshots" do
    store = start_supervised!({RoutingSnapshotStore, name: nil, limit: 2}, id: :limited)
    targets = targets(20)
    snapshot = RoutingSnapshot.new(1, targets, store: store)

    for source_id <- 2..50, do: RoutingSnapshot.new(source_id, targets, store: store)

    assert_sizes(store, 2)
    refute :ets.member(snapshot.table, snapshot.key)
    assert RoutingSnapshot.resolve(snapshot, [0]) == expected(targets, [0])
  end

  test "estimated-byte eviction bounds the store while retaining one oversized snapshot" do
    sample_targets = targets(20)
    encoded = :erlang.term_to_binary(List.to_tuple(sample_targets), compressed: 1)
    weight = :erlang.external_size(List.to_tuple(sample_targets)) + byte_size(encoded)

    store =
      start_supervised!(
        {RoutingSnapshotStore, name: nil, max_bytes: weight + 1},
        id: :byte_limited
      )

    first = RoutingSnapshot.new(1, sample_targets, store: store)
    second = RoutingSnapshot.new(2, sample_targets, store: store)

    refute :ets.member(first.table, first.key)
    assert :ets.member(second.table, second.key)
    assert_sizes(store, 1)
    assert :sys.get_state(store).estimated_bytes == second.estimated_bytes

    oversized = RoutingSnapshot.new(3, targets(100), store: store)
    assert :ets.member(oversized.table, oversized.key)
    assert_sizes(store, 1)
    assert :sys.get_state(store).estimated_bytes == oversized.estimated_bytes
  end

  test "expiry retires all indexes without invalidating readers" do
    store = start_supervised!({RoutingSnapshotStore, name: nil, ttl: 0}, id: :expired)
    targets = targets(20)
    snapshot = RoutingSnapshot.new(1, targets, store: store)
    RoutingSnapshotStore.prune(store)

    assert_sizes(store, 0)
    assert :sys.get_state(store).estimated_bytes == 0
    assert RoutingSnapshot.resolve(snapshot, [0]) == expected(targets, [0])
  end

  test "late retirement cannot remove the replacement generation", %{store: store} do
    old = RoutingSnapshot.new(1, targets(20), store: store)
    current_targets = targets(20, 2)
    current = RoutingSnapshot.new(1, current_targets, store: store)
    RoutingSnapshotStore.delete(store, old.key)
    assert_sizes(store, 1)
    assert :ets.member(current.table, current.key)

    RoutingSnapshotStore.delete(store, current.key)
    assert_sizes(store, 0)
    assert RoutingSnapshot.resolve(current, [0]) == expected(current_targets, [0])
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
    snapshot = RoutingSnapshot.new(999_999, targets(20), store: store)

    assert_receive {
      [:logflare, :rules, :routing_snapshot_store],
      %{sources: 1, estimated_bytes: bytes},
      %{action: :put}
    }

    assert bytes == snapshot.estimated_bytes
  end

  defp targets(count, generation \\ 1) do
    generation = :erlang.phash2(generation)
    for id <- 1..count, do: {id, generation * 10_000 + id, nil}
  end

  defp expected(targets, positions) do
    for position <- positions,
        is_integer(position) and position >= 0,
        target = Enum.at(targets, position),
        target != nil,
        do: target
  end

  defp assert_sizes(store, expected) do
    state = :sys.get_state(store)
    assert :ets.info(state.table, :size) == expected
    assert :ets.info(state.sources, :size) == expected
    assert :ets.info(state.expiry, :size) == expected
  end
end
