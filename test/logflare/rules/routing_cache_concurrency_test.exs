Mimic.copy(Cachex)
Mimic.copy(Logflare.Rules.Cache)
Mimic.copy(Logflare.Rules.RoutingSnapshotStore)

defmodule Logflare.Rules.RoutingCacheConcurrencyTest do
  use Logflare.DataCase

  require Cachex.Spec

  alias Logflare.Rules
  alias Logflare.Rules.Cache
  alias Logflare.Rules.RoutingSnapshot
  alias Logflare.Rules.RoutingSnapshotStore

  test "an operation captured before repair cannot bypass the repair transaction" do
    source_id = 1_900_090_003
    key = {:rules_tree_by_source_id, [source_id]}
    snapshot = RoutingSnapshot.new(source_id, snapshot_entries([{1, 10, nil}]))
    decoded = :erlang.binary_to_term(snapshot.encoded)
    Cachex.put!(Cache, key, {:cached, {[], snapshot}})
    RoutingSnapshotStore.delete(RoutingSnapshotStore, snapshot.key)
    :sys.get_state(RoutingSnapshotStore)
    parent = self()
    on_exit(fn -> Cache.bust_by(source_id: source_id) end)

    stub(Cachex, :execute, fn cache, operation ->
      if cache == Cache and Process.get(:pause_bust) do
        Mimic.call_original(Cachex, :execute, [
          cache,
          fn captured ->
            send(parent, {:bust_ready, self(), Cachex.Spec.cache(captured, :transactions)})
            receive do: (:continue_bust -> operation.(captured))
          end
        ])
      else
        Mimic.call_original(Cachex, :execute, [cache, operation])
      end
    end)

    stub(RoutingSnapshotStore, :put, fn server, id, targets, estimated_bytes ->
      if id == source_id do
        send(parent, {:repair_ready, self()})
        receive do: (:continue_repair -> :ok)
      end

      Mimic.call_original(RoutingSnapshotStore, :put, [server, id, targets, estimated_bytes])
    end)

    bust =
      Task.async(fn ->
        Process.put(:pause_bust, true)
        Cache.bust_by(source_id: source_id)
      end)

    assert_receive {:bust_ready, bust_pid, true}
    repair = Task.async(fn -> Cache.repair_routing_snapshot(source_id, snapshot, decoded) end)
    assert_receive {:repair_ready, repair_pid}
    send(bust_pid, :continue_bust)

    TestUtils.retry_assert(fn ->
      assert {:current_stacktrace, stack} = Process.info(bust_pid, :current_stacktrace)

      assert Enum.any?(stack, fn {module, function, _arity, _location} ->
               module == :gen and function == :do_call
             end)
    end)

    send(repair_pid, :continue_repair)
    assert {:repaired, _replacement} = Task.await(repair)
    assert Task.await(bust) == {:ok, 1}
    assert Cachex.get(Cache, key) == {:ok, nil}
  end

  test "a cold loader evicted before Cachex commits cannot leave its late header" do
    store = start_supervised!({RoutingSnapshotStore, name: nil, limit: 1})
    parent = self()
    first_id = 1_900_090_101
    second_id = first_id + 1
    cleanup_headers([first_id, second_id])

    stub(Rules, :rules_tree_by_source_id, fn id ->
      {[], snapshot_entries([{id, 10, nil}])}
    end)

    stub(RoutingSnapshotStore, :put, fn server, id, targets, bytes, publisher ->
      if id in [first_id, second_id] do
        assert publisher == self()

        result =
          Mimic.call_original(RoutingSnapshotStore, :put, [store, id, targets, bytes, publisher])

        if id == first_id do
          send(parent, {:registered, self(), result})
          receive do: (:publish -> :ok)
        end

        result
      else
        Mimic.call_original(RoutingSnapshotStore, :put, [server, id, targets, bytes, publisher])
      end
    end)

    reader = Task.async(fn -> Cache.rules_tree_by_source_id(first_id) end)
    assert_receive {:registered, publisher, {table, key}}
    {_tree, current} = Cache.rules_tree_by_source_id(second_id)
    refute :ets.member(table, key)
    send(publisher, :publish)
    assert {_tree, old} = Task.await(reader)
    assert old.key == key
    assert RoutingSnapshot.resolve(old, [0]) == [{first_id, 10, nil}]

    TestUtils.retry_assert(fn ->
      assert Cachex.exists?(Cache, {:rules_tree_by_source_id, [first_id]}) == {:ok, false}
      state = :sys.get_state(store)
      assert state.publishers == %{}
      assert state.estimated_bytes == current.estimated_bytes
      assert :ets.info(state.sources, :size) == 1
    end)
  end

  for published? <- [false, true] do
    @published? published?
    test "a publisher killed #{if published?, do: "after", else: "before"} committing is retired" do
      store = start_supervised!({RoutingSnapshotStore, name: nil})
      parent = self()
      source_id = 1_900_090_103
      cleanup_headers([source_id])

      {publisher, monitor} =
        spawn_monitor(fn ->
          snapshot =
            RoutingSnapshot.new(source_id, snapshot_entries([{1, 10, nil}]),
              store: store,
              publisher: self()
            )

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

      TestUtils.retry_assert(fn ->
        assert Cachex.exists?(Cache, {:rules_tree_by_source_id, [source_id]}) == {:ok, false}
        state = :sys.get_state(store)
        assert state.publishers == %{}
        assert state.estimated_bytes == 0
        refute :ets.member(snapshot.table, snapshot.key)
      end)
    end
  end

  test "late publication retirement preserves a newer header" do
    store = start_supervised!({RoutingSnapshotStore, name: nil, limit: 1})
    parent = self()
    source_id = 1_900_090_105
    cleanup_headers([source_id, source_id + 1])

    publisher =
      Task.async(fn ->
        old =
          RoutingSnapshot.new(source_id, snapshot_entries([{1, 10, nil}]),
            store: store,
            publisher: self()
          )

        send(parent, {:ready, old})
        receive do: (:publish -> :ok)
        Cachex.put!(Cache, {:rules_tree_by_source_id, [source_id]}, {:cached, {[], old}})
        send(parent, :published)
        receive do: (:finish -> :ok)
      end)

    assert_receive {:ready, old}
    old_key = old.key

    stub(Cache, :delete_routing_snapshot, fn key ->
      result = Mimic.call_original(Cache, :delete_routing_snapshot, [key])
      send(parent, {:retired, key, result})
      result
    end)

    RoutingSnapshotStore.delete(store, old_key)
    :sys.get_state(store)
    send(publisher.pid, :publish)
    assert_receive :published
    current = RoutingSnapshot.new(source_id, snapshot_entries([{1, 20, nil}]), store: store)
    cache_key = {:rules_tree_by_source_id, [source_id]}
    Cachex.put!(Cache, cache_key, {:cached, {[], current}})
    send(publisher.pid, :finish)
    Task.await(publisher)

    assert_receive {:retired, ^old_key, :stale}
    assert :sys.get_state(store).publishers == %{}
    assert Cachex.get!(Cache, cache_key) == {:cached, {[], current}}
    assert RoutingSnapshot.resolve(current, [0]) == [{1, 20, nil}]
  end

  test "completed publication remains eligible for later capacity retirement" do
    store = start_supervised!({RoutingSnapshotStore, name: nil, limit: 1})
    source_id = 1_900_090_107
    cleanup_headers([source_id, source_id + 1])

    publisher =
      Task.async(fn ->
        snapshot =
          RoutingSnapshot.new(source_id, snapshot_entries([{1, 10, nil}]),
            store: store,
            publisher: self()
          )

        Cachex.put!(Cache, {:rules_tree_by_source_id, [source_id]}, {:cached, {[], snapshot}})
      end)

    Task.await(publisher)
    TestUtils.retry_assert(fn -> assert :sys.get_state(store).publishers == %{} end)
    RoutingSnapshot.new(source_id + 1, [], store: store)

    TestUtils.retry_assert(fn ->
      assert Cachex.exists?(Cache, {:rules_tree_by_source_id, [source_id]}) == {:ok, false}
    end)
  end

  defp cleanup_headers(ids) do
    on_exit(fn -> Enum.each(ids, &Cache.bust_by(source_id: &1)) end)
  end

  defp snapshot_entries(targets), do: targets
end
