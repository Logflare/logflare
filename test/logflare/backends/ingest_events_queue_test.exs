defmodule Logflare.Backends.IngestEventQueueTest do
  use Logflare.DataCase

  alias Logflare.PubSubRates
  alias Logflare.Backends.IngestEventQueue.BroadcastWorker
  alias Logflare.Backends.IngestEventQueue.DemandWorker
  alias Logflare.Backends.IngestEventQueue.QueueJanitor
  alias Logflare.Backends.IngestEventQueue.MapperJanitor
  alias Logflare.Backends
  alias Logflare.Backends.IngestEventQueue

  test "get_table_size/1 returns nil for non-existing tables" do
    assert nil == IngestEventQueue.get_table_size({1, 2})
  end

  test "upsert_tid/1 will recreate a new ets table if tid is stale and deleted" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)

    assert {:ok, tid} = IngestEventQueue.upsert_tid({source, backend})
    :ets.delete(tid)
    assert {:ok, new_tid} = IngestEventQueue.upsert_tid({source, backend})
    assert new_tid != tid
  end

  test "get_tid/1 will return nil if tid is stale and deleted" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)

    assert {:ok, tid} = IngestEventQueue.upsert_tid({source, backend})
    assert ^tid = IngestEventQueue.get_tid({source, backend})
    :ets.delete(tid)
    assert nil == IngestEventQueue.get_tid({source, backend})
  end

  describe "with a queue" do
    setup do
      user = insert(:user)
      sb = {insert(:source, user: user), insert(:backend, user: user)}
      IngestEventQueue.upsert_tid(sb)
      [source_backend: sb]
    end

    test "object lifecycle", %{source_backend: sb} do
      le = build(:log_event)
      # insert to table
      assert :ok = IngestEventQueue.add_to_table(sb, [le])
      assert IngestEventQueue.get_table_size(sb) == 1
      # can take pending items
      assert {:ok, [_]} = IngestEventQueue.take_pending(sb, 5)
      assert IngestEventQueue.count_pending(sb) == 1
      # set to ingested
      assert {:ok, 1} = IngestEventQueue.mark_ingested(sb, [le])
      assert IngestEventQueue.count_pending(sb) == 0
      # truncate to n items
      assert :ok = IngestEventQueue.truncate(sb, :ingested, 1)
      assert IngestEventQueue.get_table_size(sb) == 1
      assert :ok = IngestEventQueue.truncate(sb, :ingested, 0)
      assert IngestEventQueue.count_pending(sb) == 0
    end

    test "truncate all events in a queue", %{source_backend: sb} do
      batch =
        for _ <- 1..500 do
          build(:log_event)
        end

      # add as pending
      assert :ok = IngestEventQueue.add_to_table(sb, batch)
      assert :ok = IngestEventQueue.truncate(sb, :all, 50)
      assert IngestEventQueue.get_table_size(sb) == 50
      assert :ok = IngestEventQueue.truncate(sb, :all, 0)
      assert IngestEventQueue.get_table_size(sb) == 0
    end

    test "truncate ingested events in a queue", %{source_backend: sb} do
      batch =
        for _ <- 1..500 do
          build(:log_event)
        end

      # add as pending
      assert :ok = IngestEventQueue.add_to_table(sb, batch)
      assert {:ok, _} = IngestEventQueue.mark_ingested(sb, batch)
      assert :ok = IngestEventQueue.truncate(sb, :ingested, 50)
      assert IngestEventQueue.get_table_size(sb) == 50
      assert IngestEventQueue.count_pending(sb) == 0
      assert :ok = IngestEventQueue.truncate(sb, :ingested, 0)
      assert IngestEventQueue.get_table_size(sb) == 0
    end

    test "truncate pending events in a queue", %{source_backend: sb} do
      batch =
        for _ <- 1..500 do
          build(:log_event)
        end

      # add as pending
      assert :ok = IngestEventQueue.add_to_table(sb, batch)
      assert :ok = IngestEventQueue.truncate(sb, :pending, 50)
      assert IngestEventQueue.count_pending(sb) == 50
      assert :ok = IngestEventQueue.truncate(sb, :pending, 0)
      assert IngestEventQueue.count_pending(sb) == 0
    end
  end

  test "BroadcastWorker broadcasts every n seconds" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)

    IngestEventQueue.upsert_tid({source, nil})
    IngestEventQueue.upsert_tid({source, backend})
    :timer.sleep(100)
    start_supervised!({BroadcastWorker, interval: 100})

    le = build(:log_event, source: source)
    IngestEventQueue.add_to_table({source, backend}, [le])
    IngestEventQueue.add_to_table({source, nil}, [le])
    :timer.sleep(300)
    assert Backends.local_buffer_len(source) == 1
    assert Backends.local_buffer_len(source, backend) == 1
    assert PubSubRates.Cache.get_cluster_buffers(source.id, backend.id) == 1
    assert PubSubRates.Cache.get_cluster_buffers(source.id, nil) == 1
  end

  test "DemandWorker with backend" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)
    IngestEventQueue.upsert_tid({source, backend})
    le = build(:log_event, source: source)
    IngestEventQueue.add_to_table({source, backend}, [le])
    start_supervised!({DemandWorker, source: source, backend: backend})
    :timer.sleep(100)
    assert {:ok, [_]} = DemandWorker.fetch({source, backend}, 5)
    assert IngestEventQueue.get_table_size({source, backend}) == 1
    assert IngestEventQueue.count_pending({source, backend}) == 0
    assert Backends.local_buffer_pending_len(source, backend) == 0
  end

  test "QueueJanitor cleans up :ingested events" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)
    IngestEventQueue.upsert_tid({source, backend})
    le = build(:log_event, source: source)
    IngestEventQueue.add_to_table({source, backend}, [le])
    IngestEventQueue.mark_ingested({source, backend}, [le])
    assert IngestEventQueue.get_table_size({source, backend}) == 1

    start_supervised!(
      {QueueJanitor, source: source, backend: backend, interval: 100, remainder: 0}
    )

    :timer.sleep(500)
    assert IngestEventQueue.get_table_size({source, backend}) == 1
    assert IngestEventQueue.count_pending({source, backend}) == 0
  end

  test "QueueJanitor drops all if exceeds max" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)
    IngestEventQueue.upsert_tid({source, backend})
    batch = for _ <- 1..105, do: build(:log_event, source: source)
    IngestEventQueue.add_to_table({source, backend}, batch)
    assert IngestEventQueue.get_table_size({source, backend}) == 105
    start_supervised!({QueueJanitor, source: source, backend: backend, interval: 100, max: 100})
    :timer.sleep(500)
    assert IngestEventQueue.get_table_size({source, backend}) == 0
  end

  test "MapperJanitor cleans up stale tids" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)
    IngestEventQueue.upsert_tid({source, backend})
    tid = IngestEventQueue.get_tid({source, backend})
    :ets.delete(tid)
    start_supervised!({MapperJanitor, interval: 100})
    :timer.sleep(500)
    assert IngestEventQueue.get_table_size({source, backend}) == nil
    assert :ets.info(:ingest_event_queue_mapping, :size) == 0
  end
end
