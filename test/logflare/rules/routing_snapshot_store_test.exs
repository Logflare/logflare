defmodule Logflare.Rules.RoutingSnapshotStoreTest do
  use ExUnit.Case, async: true

  alias Logflare.Rules.RoutingSnapshotStore, as: Store

  setup do
    store = start_supervised!({Store, name: nil, table: nil})
    %{store: store, table: :sys.get_state(store).table}
  end

  test "named table and exact generation survive a store restart" do
    opts = [name: nil, table: __MODULE__]
    store = start_supervised!({Store, opts}, id: :restart)
    old = key(1)
    assert Store.put(store, old, {{1, 10, nil}}, 100) == __MODULE__
    stop_supervised!(:restart)
    assert Store.restore(store, __MODULE__, old, encode(10), 100) == :ok
    store = start_supervised!({Store, opts}, id: :restart)
    assert :ok = Store.restore(store, __MODULE__, old, encode(10), 100)
    :sys.get_state(store)
    assert :ets.lookup_element(__MODULE__, old, 2) == {1, 10, nil}

    current = key(1)
    Store.put(store, current, {{1, 20, nil}}, 100)
    Store.restore(store, __MODULE__, old, encode(10), 100)
    :sys.get_state(store)
    refute :ets.member(__MODULE__, old)
    assert :ets.lookup_element(__MODULE__, current, 2) == {1, 20, nil}
  end

  test "duplicate and older restores are rejected before decoding and do not renew TTL", ctx do
    old = key(1)
    current = key(1)
    Store.put(ctx.store, current, {{1, 20, nil}}, 100)
    parent = self()

    :sys.replace_state(ctx.store, fn state ->
      send(parent, {:before, :ets.lookup(state.sources, 1)})
      state
    end)

    assert_receive {:before, source}
    Store.restore(ctx.store, ctx.table, old, <<>>, 100)
    :sys.get_state(ctx.store)
    Store.restore(ctx.store, ctx.table, current, <<>>, 100)
    state = :sys.get_state(ctx.store)

    :sys.replace_state(ctx.store, fn state ->
      send(parent, {:after, :ets.lookup(state.sources, 1)})
      state
    end)

    assert_receive {:after, ^source}
    assert state.estimated_bytes == 100
    refute :ets.member(ctx.table, old)
    assert :ets.lookup_element(ctx.table, current, 2) == {1, 20, nil}
  end

  test "restore coalesces concurrent requests and bounds the suspended-store mailbox" do
    store = start_supervised!({Store, name: nil, table: nil, restore_slots: 4}, id: :bounded)
    table = :sys.get_state(store).table
    key = key(1)
    :sys.suspend(store)

    try do
      for _ <- 1..1000, do: Store.restore(store, table, key, encode(10), 100)
      assert {:message_queue_len, 1} = Process.info(store, :message_queue_len)
      for id <- 2..1000, do: Store.restore(store, table, key(id), encode(10), 100)
      assert {:message_queue_len, count} = Process.info(store, :message_queue_len)
      assert count <= 4
    after
      :sys.resume(store)
    end

    state = :sys.get_state(store)
    assert :ets.info(state.requests, :size) == 0
    assert :ets.info(state.sources, :size) <= 4
    assert :ets.lookup_element(table, key, 2) == {1, 10, nil}
  end

  test "full source capacity refuses restore without evicting current acceleration" do
    store = start_supervised!({Store, name: nil, table: nil, limit: 1}, id: :full)
    current = key(2)
    table = Store.put(store, current, {{1, 20, nil}}, 100)
    old = key(1)
    Store.restore(store, table, old, <<>>, 100)
    state = :sys.get_state(store)
    refute :ets.member(table, old)
    assert :ets.member(table, current)
    assert state.estimated_bytes == 100
    assert :ets.info(state.requests, :size) == 0
  end

  test "byte pressure refuses both extra sources and oversized replacement restores" do
    store = start_supervised!({Store, name: nil, table: nil, max_bytes: 100}, id: :bytes)
    current = key(1)
    table = Store.put(store, current, {{1, 20, nil}}, 100)
    other = key(2)
    newer = key(1)
    Store.restore(store, table, other, <<>>, 1)
    :sys.get_state(store)
    Store.restore(store, table, newer, <<>>, 101)
    state = :sys.get_state(store)
    assert state.estimated_bytes == 100
    assert :ets.member(table, current)
    refute :ets.member(table, other)
    refute :ets.member(table, newer)
  end

  test "a newer fitting restore replaces the old generation without mixing targets", ctx do
    old = key(1)
    current = key(1)
    Store.put(ctx.store, old, {{1, 10, nil}}, 100)
    Store.restore(ctx.store, ctx.table, current, encode(20), 90)
    state = :sys.get_state(ctx.store)
    refute :ets.member(ctx.table, old)
    assert :ets.lookup_element(ctx.table, current, 2) == {1, 20, nil}
    assert state.estimated_bytes == 90
    assert :ets.info(state.sources, :size) == 1
    assert :ets.info(state.expiry, :size) == 1
  end

  test "generation-qualified deletion preserves a newer restored generation", ctx do
    old = key(1)
    current = key(1)
    Store.restore(ctx.store, ctx.table, current, encode(20), 100)
    :sys.get_state(ctx.store)
    Store.delete(ctx.store, old)
    :sys.get_state(ctx.store)
    assert :ets.member(ctx.table, current)
    Store.delete(ctx.store, current)
    state = :sys.get_state(ctx.store)
    refute :ets.member(ctx.table, current)
    assert state.estimated_bytes == 0
  end

  test "expiry removes restored acceleration and its accounting" do
    store = start_supervised!({Store, name: nil, table: nil, ttl: 0}, id: :expiry)
    table = :sys.get_state(store).table
    key = key(1)
    Store.restore(store, table, key, encode(10), 100)
    :sys.get_state(store)
    Store.prune(store)
    state = :sys.get_state(store)
    refute :ets.member(table, key)
    assert state.estimated_bytes == 0
    assert :ets.info(state.sources, :size) == 0
    assert :ets.info(state.expiry, :size) == 0
  end

  test "prune releases an abandoned admission claim and ignores its late request", ctx do
    state = :sys.get_state(ctx.store)
    key = key(1)
    claim = {0, key, make_ref()}
    :ets.insert(state.requests, claim)
    assert :ok = Store.prune(ctx.store)
    GenServer.cast(ctx.store, {:restore, state.requests, claim, <<>>, 100})
    :sys.get_state(ctx.store)
    refute :ets.member(ctx.table, key)
    Store.restore(ctx.store, ctx.table, key, encode(10), 100)
    :sys.get_state(ctx.store)
    assert :ets.member(ctx.table, key)
  end

  test "a restore captured before restart cannot affect the replacement store" do
    opts = [name: nil, table: __MODULE__]
    store = start_supervised!({Store, opts}, id: :restart)
    state = :sys.get_state(store)
    claim = {0, key(1), make_ref()}
    stop_supervised!(:restart)
    store = start_supervised!({Store, opts}, id: :restart)
    GenServer.cast(store, {:restore, state.requests, claim, <<>>, 100})
    assert :sys.get_state(store).estimated_bytes == 0
  end

  test "out-of-order cold publication cannot replace a newer resident generation", ctx do
    old = key(1)
    current = key(1)
    Store.put(ctx.store, current, {{1, 20, nil}}, 100)
    Store.put(ctx.store, old, {{1, 10, nil}}, 100)
    refute :ets.member(ctx.table, old)
    assert :ets.lookup_element(ctx.table, current, 2) == {1, 20, nil}
  end

  defp key(source_id), do: {source_id, :erlang.unique_integer([:monotonic, :positive])}
  defp encode(backend_id), do: :erlang.term_to_binary({{1, backend_id, nil}})
end
