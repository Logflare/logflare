Mimic.copy(Cachex)
Mimic.copy(Logflare.Rules.RoutingSnapshotStore)

defmodule Logflare.Rules.RoutingCacheConcurrencyTest do
  use Logflare.DataCase

  require Cachex.Spec

  alias Logflare.Rules.Cache
  alias Logflare.Rules.RoutingSnapshot
  alias Logflare.Rules.RoutingSnapshotStore

  test "an operation captured before repair cannot bypass the repair transaction" do
    source_id = 1_900_090_003
    key = {:rules_tree_by_source_id, [source_id]}
    snapshot = RoutingSnapshot.new(source_id, [{1, {1, 10, nil}}])
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
end
