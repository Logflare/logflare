defmodule Logflare.Backends.IngestEventQueueTest do
  use Logflare.DataCase

  alias Logflare.PubSubRates
  alias Logflare.Backends.IngestEventQueue.BufferCacheWorker
  alias Logflare.Backends.IngestEventQueue.QueueJanitor
  alias Logflare.Backends.IngestEventQueue.MapperJanitor
  alias Logflare.Backends
  alias Logflare.Backends.IngestEventQueue

  setup do
    insert(:plan)
    :ok
  end

  test "get_table_size/1 returns nil for non-existing tables" do
    assert nil == IngestEventQueue.get_table_size({1, 2, 4})
  end

  test "upsert_tid/1 will recreate a new ets table if tid is stale and deleted" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)
    pid = self()
    assert {:ok, tid} = IngestEventQueue.upsert_tid({source.id, backend.id, pid})
    :ets.delete(tid)
    assert {:ok, new_tid} = IngestEventQueue.upsert_tid({source.id, backend.id, pid})
    assert new_tid != tid
  end

  test "get_tid/1 will return nil if tid is stale and deleted" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)
    pid = self()
    assert {:ok, tid} = IngestEventQueue.upsert_tid({source.id, backend.id, pid})
    assert ^tid = IngestEventQueue.get_tid({source.id, backend.id, pid})
    :ets.delete(tid)
    assert nil == IngestEventQueue.get_tid({source.id, backend.id, pid})
  end

  describe "with user, source, backend" do
    setup do
      user = insert(:user)
      [source: insert(:source, user: user), backend: insert(:backend, user: user)]
    end

    test "list_queues/1 returns list of table keys", %{source: source, backend: backend} do
      IngestEventQueue.upsert_tid({source.id, backend.id, :erlang.list_to_pid(~c"<0.12.34>")})
      IngestEventQueue.upsert_tid({source.id, backend.id, :erlang.list_to_pid(~c"<0.12.35>")})
      IngestEventQueue.upsert_tid({source.id, backend.id, :erlang.list_to_pid(~c"<0.12.36>")})
      assert IngestEventQueue.list_queues({source.id, backend.id}) |> length() == 3
    end

    test "list_pending_counts/1 returns list of counts", %{source: source, backend: backend} do
      key = {source.id, backend.id, self()}
      IngestEventQueue.upsert_tid(key)
      le = build(:log_event, source: source)
      IngestEventQueue.add_to_table(key, [le])
      assert [{_, 1}] = IngestEventQueue.list_pending_counts({source.id, backend.id})

      IngestEventQueue.mark_ingested(key, [le])
      assert [{_, 0}] = IngestEventQueue.list_pending_counts({source.id, backend.id})
    end

    test "list_pending_counts/1 does not include uninitialized tables", %{
      source: source,
      backend: backend
    } do
      assert {:ok, tid} = IngestEventQueue.upsert_tid({source.id, backend.id, self()})
      # kill the table
      :ets.delete(tid)
      assert [] == IngestEventQueue.list_pending_counts({source.id, backend.id})
    end

    test "list_counts/1 returns list of counts", %{
      source: %{id: source_id} = source,
      backend: %{id: backend_id}
    } do
      pid = self()
      key = {source_id, backend_id, pid}
      assert {:ok, _tid} = IngestEventQueue.upsert_tid(key)

      le = build(:log_event, source: source)
      IngestEventQueue.add_to_table(key, [le])
      IngestEventQueue.mark_ingested(key, [le])

      assert [
               {{^source_id, ^backend_id, ^pid}, 1}
             ] =
               IngestEventQueue.list_counts({source_id, backend_id})
    end

    test "list_counts/1 does not include uninitialized tables", %{source: source} do
      assert {:ok, tid} = IngestEventQueue.upsert_tid({source.id, nil, self()})
      # kill the table
      :ets.delete(tid)
      assert [] == IngestEventQueue.list_counts({source.id, nil})
    end

    test "add_to_table/2 falls back to the startup queue when a producer table died mid-dispatch",
         %{source: %{id: source_id} = source, backend: %{id: backend_id}} do
      pid = self()
      producer_key = {source_id, backend_id, pid}
      startup_key = {source_id, backend_id, nil}

      assert {:ok, producer_tid} = IngestEventQueue.upsert_tid(producer_key)
      assert {:ok, _} = IngestEventQueue.upsert_tid(startup_key)

      # Simulate the owning producer dying after its tid was resolved but before
      # the insert ran. Driving the {key, tid} clause directly reproduces that
      # window: get_tid would otherwise filter the dead table out first.
      :ets.delete(producer_tid)

      le = build(:log_event, source: source)
      assert :ok = IngestEventQueue.add_to_table({producer_key, producer_tid}, [le])

      assert IngestEventQueue.total_pending(startup_key) == 1
    end

    test "add_to_table/3 does not recurse when the startup queue table is stale", %{
      source: %{id: source_id} = source,
      backend: %{id: backend_id}
    } do
      startup_key = {source_id, backend_id, nil}
      assert {:ok, startup_tid} = IngestEventQueue.upsert_tid(startup_key)
      :ets.delete(startup_tid)

      # Drive the insert clause directly with the stale startup-queue tid; the
      # fallback must bottom out instead of looping on {_, _, nil}.
      assert {:error, :not_initialized} =
               IngestEventQueue.add_to_table(
                 {startup_key, startup_tid},
                 [build(:log_event, source: source)]
               )
    end

    test "queues_pending_size/1 returns counts across all queues", %{
      source: %{id: source_id} = source,
      backend: %{id: backend_id}
    } do
      pid = self()
      IngestEventQueue.upsert_tid({source_id, backend_id, pid})
      IngestEventQueue.upsert_tid({source_id, backend_id, nil})
      IngestEventQueue.upsert_tid({source_id, nil, nil})

      IngestEventQueue.add_to_table({source_id, backend_id, nil}, [
        build(:log_event, source: source)
      ])

      IngestEventQueue.add_to_table({source_id, backend_id, self()}, [
        build(:log_event, source: source)
      ])

      assert IngestEventQueue.queues_pending_size({source_id, backend_id}) == 2

      IngestEventQueue.add_to_table({source_id, nil, nil}, [build(:log_event, source: source)])
      assert IngestEventQueue.queues_pending_size({source_id, nil}) == 1
    end
  end

  describe "consolidated keys" do
    setup do
      user = insert(:user)
      [source: insert(:source, user: user), backend: insert(:backend, user: user)]
    end

    test "consolidated_key?/1 returns true for consolidated queue keys" do
      assert IngestEventQueue.consolidated_key?({:consolidated, 123})
      assert IngestEventQueue.consolidated_key?({:consolidated, 123, nil})
      assert IngestEventQueue.consolidated_key?({:consolidated, 123, self()})
    end

    test "consolidated_key?/1 returns false for regular keys" do
      refute IngestEventQueue.consolidated_key?({1, 2})
      refute IngestEventQueue.consolidated_key?({1, 2, nil})
      refute IngestEventQueue.consolidated_key?({1, 2, self()})
      refute IngestEventQueue.consolidated_key?(:other)
    end

    test "upsert_tid/1 and get_tid/1 work with consolidated keys", %{backend: backend} do
      key = {:consolidated, backend.id, self()}
      assert {:ok, tid} = IngestEventQueue.upsert_tid(key)
      assert ^tid = IngestEventQueue.get_tid(key)
    end

    test "upsert_tid/1 returns error if tid already exists for consolidated key", %{
      backend: backend
    } do
      key = {:consolidated, backend.id, self()}
      assert {:ok, tid} = IngestEventQueue.upsert_tid(key)
      assert {:error, :already_exists, ^tid} = IngestEventQueue.upsert_tid(key)
    end

    test "list_queues/1 returns consolidated queues", %{backend: backend} do
      IngestEventQueue.upsert_tid({:consolidated, backend.id, :erlang.list_to_pid(~c"<0.12.34>")})
      IngestEventQueue.upsert_tid({:consolidated, backend.id, :erlang.list_to_pid(~c"<0.12.35>")})

      queues = IngestEventQueue.list_queues({:consolidated, backend.id})
      assert length(queues) == 2

      assert Enum.all?(queues, fn
               {:consolidated, bid, pid} when is_integer(bid) and is_pid(pid) -> true
               _ -> false
             end)
    end

    test "list_pending_counts/1 returns counts for consolidated queues", %{
      source: source,
      backend: backend
    } do
      key = {:consolidated, backend.id, self()}
      IngestEventQueue.upsert_tid(key)
      le = build(:log_event, source: source)
      IngestEventQueue.add_to_table(key, [le])

      assert [{^key, 1}] = IngestEventQueue.list_pending_counts({:consolidated, backend.id})
    end

    test "add_to_table/2 works with consolidated queue key", %{
      source: source,
      backend: backend
    } do
      key = {:consolidated, backend.id, self()}
      IngestEventQueue.upsert_tid(key)
      le = build(:log_event, source: source)

      assert :ok = IngestEventQueue.add_to_table(key, [le])
      assert {:ok, [^le]} = IngestEventQueue.take_pending(key, 1)
    end

    test "add_to_table/2 distributes to startup queue when no active queues", %{
      source: source,
      backend: backend
    } do
      startup_key = {:consolidated, backend.id, nil}
      IngestEventQueue.upsert_tid(startup_key)
      le = build(:log_event, source: source)

      assert :ok = IngestEventQueue.add_to_table({:consolidated, backend.id}, [le])
      assert IngestEventQueue.total_pending(startup_key) == 1
    end

    test "queues_pending_size/1 returns total for consolidated queues", %{
      source: source,
      backend: backend
    } do
      key1 = {:consolidated, backend.id, :erlang.list_to_pid(~c"<0.12.34>")}
      key2 = {:consolidated, backend.id, :erlang.list_to_pid(~c"<0.12.35>")}
      IngestEventQueue.upsert_tid(key1)
      IngestEventQueue.upsert_tid(key2)

      IngestEventQueue.add_to_table(key1, [build(:log_event, source: source)])

      IngestEventQueue.add_to_table(key2, [
        build(:log_event, source: source),
        build(:log_event, source: source)
      ])

      assert IngestEventQueue.queues_pending_size({:consolidated, backend.id}) == 3
    end

    test "delete_queue/1 works with consolidated keys", %{source: source, backend: backend} do
      key = {:consolidated, backend.id, self()}
      IngestEventQueue.upsert_tid(key)
      IngestEventQueue.add_to_table(key, [build(:log_event, source: source)])

      assert :ok = IngestEventQueue.delete_queue(key)
      assert is_nil(IngestEventQueue.get_tid(key))
    end
  end

  describe "startup queue" do
    setup do
      user = insert(:user)
      sbp = {insert(:source, user: user).id, insert(:backend, user: user).id, nil}
      IngestEventQueue.upsert_tid(sbp)
      [queue: sbp]
    end

    test "adding to startup queue", %{queue: {sid, bid, _} = queue} do
      le = build(:log_event, message: "123")
      assert :ok = IngestEventQueue.add_to_table(queue, [le])
      assert IngestEventQueue.total_pending(queue) == 1
      assert IngestEventQueue.total_pending({sid, bid}) == 1
      assert IngestEventQueue.total_pending({sid, bid, nil}) == 1
    end

    test "move/1 moves all events from one queue to target queue", %{queue: {sid, bid, _} = queue} do
      target = {sid, bid, self()}
      le = build(:log_event, message: "123")
      IngestEventQueue.upsert_tid(target)
      assert :ok = IngestEventQueue.add_to_table(queue, [le])
      assert IngestEventQueue.total_pending(queue) == 1
      assert IngestEventQueue.total_pending(target) == 0
      assert {:ok, 1} = IngestEventQueue.move(queue, target)
      assert IngestEventQueue.total_pending(queue) == 0
      assert IngestEventQueue.total_pending(target) == 1
      assert IngestEventQueue.total_pending({sid, bid}) == 1
      assert IngestEventQueue.total_pending({sid, bid, nil}) == 0
    end
  end

  describe "with a queue" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      sbp = {source.id, insert(:backend, user: user).id, self()}
      IngestEventQueue.upsert_tid(sbp)
      [source: source, source_backend_pid: sbp]
    end

    test "object lifecycle", %{source: source, source_backend_pid: sbp} do
      le = build(:log_event, source: source)
      # insert to table
      assert :ok = IngestEventQueue.add_to_table(sbp, [le])
      assert IngestEventQueue.get_table_size(sbp) == 1
      # can take pending items
      assert {:ok, [_]} = IngestEventQueue.take_pending(sbp, 5)
      assert IngestEventQueue.total_pending(sbp) == 1
      # set to ingested
      assert {:ok, 1} = IngestEventQueue.mark_ingested(sbp, [le])
      assert IngestEventQueue.total_pending(sbp) == 0
      # truncate to n items
      assert :ok = IngestEventQueue.truncate_table(sbp, :ingested, 1)
      assert IngestEventQueue.get_table_size(sbp) == 1
      assert :ok = IngestEventQueue.truncate_table(sbp, :ingested, 0)
      assert IngestEventQueue.total_pending(sbp) == 0
    end

    test "drop n items from a queue", %{source: source, source_backend_pid: sbp} do
      batch = for _ <- 1..500, do: build(:log_event, source: source)
      assert :ok = IngestEventQueue.add_to_table(sbp, batch)
      assert {:ok, 2} = IngestEventQueue.drop(sbp, :all, 2)
      assert IngestEventQueue.get_table_size(sbp) == 498
      assert {:ok, 0} = IngestEventQueue.drop(sbp, :ingested, 2)
      assert IngestEventQueue.get_table_size(sbp) == 498
    end

    test "truncate all events in a queue", %{source: source, source_backend_pid: sbp} do
      batch =
        for _ <- 1..500 do
          build(:log_event, source: source)
        end

      # add as pending
      assert :ok = IngestEventQueue.add_to_table(sbp, batch)
      assert :ok = IngestEventQueue.truncate_table(sbp, :all, 50)
      assert IngestEventQueue.get_table_size(sbp) == 50
      assert :ok = IngestEventQueue.truncate_table(sbp, :all, 0)
      assert IngestEventQueue.get_table_size(sbp) == 0
    end

    test "truncate ingested events in a queue", %{source: source, source_backend_pid: sbp} do
      batch =
        for _ <- 1..500 do
          build(:log_event, source: source)
        end

      # add as pending
      assert :ok = IngestEventQueue.add_to_table(sbp, batch)
      assert {:ok, _} = IngestEventQueue.mark_ingested(sbp, batch)
      assert :ok = IngestEventQueue.truncate_table(sbp, :ingested, 50)
      assert IngestEventQueue.get_table_size(sbp) == 50
      assert IngestEventQueue.total_pending(sbp) == 0
      assert :ok = IngestEventQueue.truncate_table(sbp, :ingested, 0)
      assert IngestEventQueue.get_table_size(sbp) == 0
    end

    test "truncate pending events in a queue", %{source: source, source_backend_pid: sbp} do
      batch =
        for _ <- 1..500 do
          build(:log_event, source: source)
        end

      # add as pending
      assert :ok = IngestEventQueue.add_to_table(sbp, batch)
      assert :ok = IngestEventQueue.truncate_table(sbp, :pending, 50)
      assert IngestEventQueue.total_pending(sbp) == 50
      assert :ok = IngestEventQueue.truncate_table(sbp, :pending, 0)
      assert IngestEventQueue.total_pending(sbp) == 0
    end
  end

  describe "truncate_tid/3 with a stale (reclaimed) table" do
    setup do
      # A tid whose ETS table has been reclaimed, simulating the owning
      # producer dying after truncate_table/3 resolved the tid via get_tid/1
      # but before the ETS operations run.
      tid = :ets.new(:stale_truncate_queue, [:public, :set])
      :ets.delete(tid)
      [stale_tid: tid]
    end

    test "nil tid returns :not_initialized without raising" do
      assert {:error, :not_initialized} = IngestEventQueue.truncate_tid(nil, :all, 0)
      assert {:error, :not_initialized} = IngestEventQueue.truncate_tid(nil, :pending, 100)
    end

    test ":all/0 truncate tolerates the reclaimed table and emits telemetry", %{
      stale_tid: stale_tid
    } do
      ref = make_ref()

      :telemetry.attach(
        "test-stale-table-#{inspect(ref)}",
        [:logflare, :ingest_event_queue, :stale_table],
        fn _event, measurements, _meta, pid -> send(pid, {:telemetry, measurements}) end,
        self()
      )

      assert {:error, :not_initialized} = IngestEventQueue.truncate_tid(stale_tid, :all, 0)
      assert_receive {:telemetry, %{count: 1}}

      :telemetry.detach("test-stale-table-#{inspect(ref)}")
    end

    test "size-bounded truncate tolerates the reclaimed table", %{stale_tid: stale_tid} do
      assert {:error, :not_initialized} = IngestEventQueue.truncate_tid(stale_tid, :all, 50)
      assert {:error, :not_initialized} = IngestEventQueue.truncate_tid(stale_tid, :pending, 100)
      assert {:error, :not_initialized} = IngestEventQueue.truncate_tid(stale_tid, :ingested, 0)
    end

    test "truncate_table/3 returns :not_initialized when the queue table is gone" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      sbp = {source.id, backend.id, self()}
      assert {:ok, tid} = IngestEventQueue.upsert_tid(sbp)
      :ets.delete(tid)

      assert {:error, :not_initialized} = IngestEventQueue.truncate_table(sbp, :all, 0)
      assert {:error, :not_initialized} = IngestEventQueue.truncate_table(sbp, :pending, 100)
    end
  end

  describe "take_pending_ids/2" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      sbp = {source.id, insert(:backend, user: user).id, self()}
      IngestEventQueue.upsert_tid(sbp)
      [source: source, sbp: sbp]
    end

    test "claims pending events, marking them :processing and returning {id, size} pairs", %{
      source: source,
      sbp: sbp
    } do
      events = for _ <- 1..5, do: build(:log_event, source: source)
      assert :ok = IngestEventQueue.add_to_table(sbp, events)

      assert {:ok, pairs, tid} = IngestEventQueue.take_pending_ids(sbp, 5)
      assert tid != nil
      assert length(pairs) == 5

      taken_ids = for {id, _size} <- pairs, do: id
      assert Enum.sort(taken_ids) == Enum.sort(for e <- events, do: e.id)
      assert IngestEventQueue.total_pending(sbp) == 0
    end

    test "respects the requested count and leaves the remainder pending", %{
      source: source,
      sbp: sbp
    } do
      events = for _ <- 1..10, do: build(:log_event, source: source)
      assert :ok = IngestEventQueue.add_to_table(sbp, events)

      assert {:ok, pairs, _tid} = IngestEventQueue.take_pending_ids(sbp, 4)
      assert length(pairs) == 4
      assert IngestEventQueue.total_pending(sbp) == 6
    end

    test "does not re-claim events already taken across sequential calls", %{
      source: source,
      sbp: sbp
    } do
      events = for _ <- 1..10, do: build(:log_event, source: source)
      assert :ok = IngestEventQueue.add_to_table(sbp, events)

      assert {:ok, first, _} = IngestEventQueue.take_pending_ids(sbp, 4)
      assert {:ok, second, _} = IngestEventQueue.take_pending_ids(sbp, 10)

      first_ids = MapSet.new(first, fn {id, _} -> id end)
      second_ids = MapSet.new(second, fn {id, _} -> id end)

      assert MapSet.disjoint?(first_ids, second_ids)
      assert MapSet.size(first_ids) == 4
      assert MapSet.size(second_ids) == 6
    end

    test "re-claims an event after it is re-added to the queue (claim counter resets)", %{
      source: source,
      sbp: sbp
    } do
      [event] = events = [build(:log_event, source: source)]
      assert :ok = IngestEventQueue.add_to_table(sbp, events)
      event_id = event.id

      assert {:ok, [{^event_id, _size}], _tid} = IngestEventQueue.take_pending_ids(sbp, 1)
      assert {:ok, [], _} = IngestEventQueue.take_pending_ids(sbp, 1)

      # re-adding (the BigQuery requeue path) resets the claim counter, making it claimable
      assert :ok = IngestEventQueue.add_to_table(sbp, events)
      assert {:ok, [{^event_id, _size}], _tid} = IngestEventQueue.take_pending_ids(sbp, 1)
    end

    # Regression guard for the concurrent-claim CAS. For correct code this passes
    # deterministically — the update_counter 0 -> 1 winner is the sole claimer, so no
    # event is ever taken twice regardless of scheduling. The large batch and several
    # concurrent claimers exist to make a *regression* (e.g. reverting to an
    # unconditional update_element) actually hit the race window and fail, rather than
    # slip through. Tagged :race so it can be isolated, but it is safe to run by default.
    @tag :race
    test "concurrent claims on the same queue never claim an event twice", %{
      source: source,
      sbp: sbp
    } do
      count = 1_000
      events = for _ <- 1..count, do: build(:log_event, source: source)
      assert :ok = IngestEventQueue.add_to_table(sbp, events)

      claimed =
        for _ <- 1..8 do
          Task.async(fn ->
            {:ok, pairs, _tid} = IngestEventQueue.take_pending_ids(sbp, count)
            for {id, _size} <- pairs, do: id
          end)
        end
        |> Task.await_many(10_000)
        |> List.flatten()

      assert length(claimed) == length(Enum.uniq(claimed))
      assert length(claimed) == count
      assert IngestEventQueue.total_pending(sbp) == 0
    end
  end

  describe "`add_to_table/3` distribution with queues_key" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      [source: source, backend: backend]
    end

    test "distributes events to active queues, skipping startup queue", %{
      source: source,
      backend: backend
    } do
      startup_key = {source.id, backend.id, nil}
      pid1 = :erlang.list_to_pid(~c"<0.100.1>")
      pid2 = :erlang.list_to_pid(~c"<0.100.2>")
      active_key1 = {source.id, backend.id, pid1}
      active_key2 = {source.id, backend.id, pid2}

      IngestEventQueue.upsert_tid(startup_key)
      IngestEventQueue.upsert_tid(active_key1)
      IngestEventQueue.upsert_tid(active_key2)

      events = build_list(200, :log_event)
      :ok = IngestEventQueue.add_to_table({source.id, backend.id}, events, chunk_size: 50)

      startup_size = IngestEventQueue.get_table_size(startup_key)
      active1_size = IngestEventQueue.get_table_size(active_key1)
      active2_size = IngestEventQueue.get_table_size(active_key2)

      assert startup_size == 0
      assert active1_size + active2_size == 200
      assert active1_size > 0
      assert active2_size > 0
    end

    test "falls back to startup queue when no active queues exist", %{
      source: source,
      backend: backend
    } do
      startup_key = {source.id, backend.id, nil}
      IngestEventQueue.upsert_tid(startup_key)

      events = build_list(5, :log_event)
      :ok = IngestEventQueue.add_to_table({source.id, backend.id}, events)

      assert IngestEventQueue.get_table_size(startup_key) == 5
    end

    test "skips queues at max capacity when `check_queue_size` is true", %{
      source: source,
      backend: backend
    } do
      pid1 = :erlang.list_to_pid(~c"<0.100.1>")
      pid2 = :erlang.list_to_pid(~c"<0.100.2>")
      full_queue = {source.id, backend.id, pid1}
      available_queue = {source.id, backend.id, pid2}

      IngestEventQueue.upsert_tid(full_queue)
      IngestEventQueue.upsert_tid(available_queue)

      max_size = IngestEventQueue.max_queue_size()
      full_batch = build_list(max_size, :log_event, source: source)
      :ok = IngestEventQueue.add_to_table(full_queue, full_batch)

      assert IngestEventQueue.get_table_size(full_queue) == max_size

      new_events = build_list(10, :log_event, source: source)

      :ok =
        IngestEventQueue.add_to_table({source.id, backend.id}, new_events, check_queue_size: true)

      assert IngestEventQueue.get_table_size(full_queue) == max_size
      assert IngestEventQueue.get_table_size(available_queue) == 10
    end

    test "distributes to all queues when `check_queue_size` is false", %{
      source: source,
      backend: backend
    } do
      pid1 = :erlang.list_to_pid(~c"<0.100.1>")
      pid2 = :erlang.list_to_pid(~c"<0.100.2>")
      queue1 = {source.id, backend.id, pid1}
      queue2 = {source.id, backend.id, pid2}

      IngestEventQueue.upsert_tid(queue1)
      IngestEventQueue.upsert_tid(queue2)

      events = build_list(200, :log_event)

      :ok =
        IngestEventQueue.add_to_table({source.id, backend.id}, events,
          check_queue_size: false,
          chunk_size: 50
        )

      size1 = IngestEventQueue.get_table_size(queue1)
      size2 = IngestEventQueue.get_table_size(queue2)

      assert size1 + size2 == 200
      assert size1 > 0
      assert size2 > 0
    end

    test "uses `no_get_tid: true` by default for round-robin with tids", %{
      source: source,
      backend: backend
    } do
      pid1 = :erlang.list_to_pid(~c"<0.100.1>")
      pid2 = :erlang.list_to_pid(~c"<0.100.2>")
      queue1 = {source.id, backend.id, pid1}
      queue2 = {source.id, backend.id, pid2}

      IngestEventQueue.upsert_tid(queue1)
      IngestEventQueue.upsert_tid(queue2)

      events = build_list(10, :log_event)
      :ok = IngestEventQueue.add_to_table({source.id, backend.id}, events)

      size1 = IngestEventQueue.get_table_size(queue1)
      size2 = IngestEventQueue.get_table_size(queue2)

      assert size1 + size2 == 10
    end

    test "uses `no_get_tid: false` path for round-robin without tids", %{
      source: source,
      backend: backend
    } do
      pid1 = :erlang.list_to_pid(~c"<0.100.1>")
      pid2 = :erlang.list_to_pid(~c"<0.100.2>")
      queue1 = {source.id, backend.id, pid1}
      queue2 = {source.id, backend.id, pid2}

      IngestEventQueue.upsert_tid(queue1)
      IngestEventQueue.upsert_tid(queue2)

      events = build_list(10, :log_event)
      :ok = IngestEventQueue.add_to_table({source.id, backend.id}, events, no_get_tid: false)

      size1 = IngestEventQueue.get_table_size(queue1)
      size2 = IngestEventQueue.get_table_size(queue2)

      assert size1 + size2 == 10
    end
  end

  describe "`add_to_table/3` distribution with consolidated queues_key" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      [source: source, backend: backend]
    end

    test "distributes events to active consolidated queues, skipping startup queue", %{
      source: source,
      backend: backend
    } do
      startup_key = {:consolidated, backend.id, nil}
      pid1 = :erlang.list_to_pid(~c"<0.100.1>")
      pid2 = :erlang.list_to_pid(~c"<0.100.2>")
      active_key1 = {:consolidated, backend.id, pid1}
      active_key2 = {:consolidated, backend.id, pid2}

      IngestEventQueue.upsert_tid(startup_key)
      IngestEventQueue.upsert_tid(active_key1)
      IngestEventQueue.upsert_tid(active_key2)

      events = build_list(200, :log_event, source: source)
      :ok = IngestEventQueue.add_to_table({:consolidated, backend.id}, events, chunk_size: 50)

      startup_size = IngestEventQueue.get_table_size(startup_key)
      active1_size = IngestEventQueue.get_table_size(active_key1)
      active2_size = IngestEventQueue.get_table_size(active_key2)

      assert startup_size == 0
      assert active1_size + active2_size == 200
      assert active1_size > 0
      assert active2_size > 0
    end

    test "falls back to startup queue when no active consolidated queues exist", %{
      source: source,
      backend: backend
    } do
      startup_key = {:consolidated, backend.id, nil}
      IngestEventQueue.upsert_tid(startup_key)

      events = build_list(5, :log_event, source: source)
      :ok = IngestEventQueue.add_to_table({:consolidated, backend.id}, events)

      assert IngestEventQueue.get_table_size(startup_key) == 5
    end

    test "skips consolidated queues at max capacity when `check_queue_size` is true", %{
      source: source,
      backend: backend
    } do
      pid1 = :erlang.list_to_pid(~c"<0.100.1>")
      pid2 = :erlang.list_to_pid(~c"<0.100.2>")
      full_queue = {:consolidated, backend.id, pid1}
      available_queue = {:consolidated, backend.id, pid2}

      IngestEventQueue.upsert_tid(full_queue)
      IngestEventQueue.upsert_tid(available_queue)

      max_size = IngestEventQueue.max_queue_size()
      full_batch = build_list(max_size, :log_event, source: source)
      :ok = IngestEventQueue.add_to_table(full_queue, full_batch)

      assert IngestEventQueue.get_table_size(full_queue) == max_size

      new_events = build_list(10, :log_event, source: source)

      :ok =
        IngestEventQueue.add_to_table({:consolidated, backend.id}, new_events,
          check_queue_size: true
        )

      assert IngestEventQueue.get_table_size(full_queue) == max_size
      assert IngestEventQueue.get_table_size(available_queue) == 10
    end

    test "uses `no_get_tid: false` path for consolidated queues", %{backend: backend} do
      pid1 = :erlang.list_to_pid(~c"<0.100.1>")
      pid2 = :erlang.list_to_pid(~c"<0.100.2>")
      queue1 = {:consolidated, backend.id, pid1}
      queue2 = {:consolidated, backend.id, pid2}

      IngestEventQueue.upsert_tid(queue1)
      IngestEventQueue.upsert_tid(queue2)

      events = build_list(10, :log_event)
      :ok = IngestEventQueue.add_to_table({:consolidated, backend.id}, events, no_get_tid: false)

      size1 = IngestEventQueue.get_table_size(queue1)
      size2 = IngestEventQueue.get_table_size(queue2)

      assert size1 + size2 == 10
    end
  end

  describe "pop_pending/2" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      key = {source.id, backend.id, self()}
      IngestEventQueue.upsert_tid(key)
      [key: key, source: source, backend: backend]
    end

    test "returns events and removes them from the queue", %{key: key, source: source} do
      events = for _ <- 1..5, do: build(:log_event, source: source)
      :ok = IngestEventQueue.add_to_table(key, events)

      assert IngestEventQueue.get_table_size(key) == 5
      assert IngestEventQueue.total_pending(key) == 5

      assert {:ok, popped} = IngestEventQueue.pop_pending(key, 3)
      assert length(popped) == 3

      assert IngestEventQueue.get_table_size(key) == 2
      assert IngestEventQueue.total_pending(key) == 2
    end

    test "returns empty list when no pending events", %{key: key} do
      assert {:ok, []} = IngestEventQueue.pop_pending(key, 10)
    end

    test "returns error when queue not initialized" do
      assert {:error, :not_initialized} = IngestEventQueue.pop_pending({999, 999, self()}, 5)
    end

    test "returns all events when requesting more than available", %{
      key: key,
      source: source
    } do
      events = for _ <- 1..3, do: build(:log_event, source: source)
      :ok = IngestEventQueue.add_to_table(key, events)

      assert {:ok, popped} = IngestEventQueue.pop_pending(key, 10)
      assert length(popped) == 3
      assert IngestEventQueue.get_table_size(key) == 0
    end

    test "only pops pending events, not ingested", %{key: key, source: source} do
      pending_events = for _ <- 1..3, do: build(:log_event, source: source)
      ingested_events = for _ <- 1..2, do: build(:log_event, source: source)

      :ok = IngestEventQueue.add_to_table(key, pending_events ++ ingested_events)
      {:ok, _} = IngestEventQueue.mark_ingested(key, ingested_events)

      assert IngestEventQueue.get_table_size(key) == 5
      assert IngestEventQueue.total_pending(key) == 3

      assert {:ok, popped} = IngestEventQueue.pop_pending(key, 10)
      assert length(popped) == 3

      assert IngestEventQueue.get_table_size(key) == 2
      assert IngestEventQueue.total_pending(key) == 0
    end

    test "works with consolidated keys", %{source: source, backend: backend} do
      consolidated_key = {:consolidated, backend.id, self()}
      IngestEventQueue.upsert_tid(consolidated_key)

      events = for _ <- 1..5, do: build(:log_event, source: source)
      :ok = IngestEventQueue.add_to_table(consolidated_key, events)

      assert {:ok, popped} = IngestEventQueue.pop_pending(consolidated_key, 3)
      assert length(popped) == 3
      assert IngestEventQueue.get_table_size(consolidated_key) == 2
    end

    test "pop_pending with 0 returns empty list", %{key: key, source: source} do
      events = for _ <- 1..5, do: build(:log_event, source: source)
      :ok = IngestEventQueue.add_to_table(key, events)

      assert {:ok, []} = IngestEventQueue.pop_pending(key, 0)
      assert IngestEventQueue.get_table_size(key) == 5
    end
  end

  test "BufferCacheWorker caches buffer lengths every n seconds" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)
    pid = self()

    table = {source.id, backend.id, pid}
    other_table = {source.id, nil, pid}

    IngestEventQueue.upsert_tid(table)
    IngestEventQueue.upsert_tid(other_table)
    start_supervised!({BufferCacheWorker, interval: 100})

    le = build(:log_event, source: source)
    IngestEventQueue.add_to_table(table, [le])
    IngestEventQueue.add_to_table(other_table, [le])
    :timer.sleep(300)

    # Verify worker cached the values automatically (without manual cache calls)
    assert PubSubRates.Cache.get_cluster_buffers(source.id, backend.id) == 1
    assert PubSubRates.Cache.get_cluster_buffers(source.id, nil) == 1
  end

  test "QueueJanitor cleans up :ingested events" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)
    pid = self()

    table = {source.id, backend.id, pid}

    IngestEventQueue.upsert_tid(table)
    le = build(:log_event, source: source)
    IngestEventQueue.add_to_table(table, [le])
    IngestEventQueue.mark_ingested(table, [le])
    assert IngestEventQueue.get_table_size(table) == 1

    start_supervised!(
      {QueueJanitor, source: source, backend: backend, interval: 50, remainder: 0}
    )

    :timer.sleep(550)
    assert IngestEventQueue.get_table_size(table) == 0
    assert IngestEventQueue.total_pending(table) == 0
  end

  test "QueueJanitor leaves remainder of :ingested events if default backend" do
    user = insert(:user)
    source = insert(:source, user: user)
    pid = self()

    table = {source.id, nil, pid}

    IngestEventQueue.upsert_tid(table)
    le = build(:log_event, source: source)
    IngestEventQueue.add_to_table(table, [le])
    IngestEventQueue.mark_ingested(table, [le])
    assert IngestEventQueue.get_table_size(table) == 1

    start_supervised!(
      {QueueJanitor,
       source: source, backend: %Backends.Backend{id: nil}, interval: 50, remainder: 10}
    )

    :timer.sleep(550)
    assert IngestEventQueue.get_table_size(table) == 1
    assert IngestEventQueue.total_pending(table) == 0
  end

  test "QueueJanitor cleans up all :ingested events if not default backend" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)
    pid = self()

    table = {source.id, backend.id, pid}

    IngestEventQueue.upsert_tid(table)
    le = build(:log_event, source: source)
    IngestEventQueue.add_to_table(table, [le])
    IngestEventQueue.mark_ingested(table, [le])
    assert IngestEventQueue.get_table_size(table) == 1

    start_supervised!(
      {QueueJanitor, source: source, backend: backend, interval: 50, remainder: 10}
    )

    :timer.sleep(550)
    assert IngestEventQueue.get_table_size(table) == 0
    assert IngestEventQueue.total_pending(table) == 0
  end

  test "QueueJanitor purges if exceeds max" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)
    pid = self()
    IngestEventQueue.upsert_tid({source.id, backend.id, pid})
    batch = for _ <- 1..105, do: build(:log_event, source: source)
    IngestEventQueue.add_to_table({source.id, backend.id, pid}, batch)
    assert IngestEventQueue.get_table_size({source.id, backend.id, pid}) == 105

    start_supervised!(
      {QueueJanitor, source: source, backend: backend, interval: 50, max: 100, purge_ratio: 1.0}
    )

    :timer.sleep(550)
    assert IngestEventQueue.get_table_size({source.id, backend.id, pid}) == 0
  end

  test "QueueJanitor purges based on purge ratio" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)
    pid = self()
    IngestEventQueue.upsert_tid({source.id, backend.id, pid})
    batch = for _ <- 1..100, do: build(:log_event, source: source)
    IngestEventQueue.add_to_table({source.id, backend.id, pid}, batch)
    assert IngestEventQueue.get_table_size({source.id, backend.id, pid}) == 100

    start_supervised!(
      {QueueJanitor, source: source, backend: backend, interval: 50, max: 90, purge_ratio: 0.5}
    )

    :timer.sleep(550)
    assert IngestEventQueue.get_table_size({source.id, backend.id, pid}) == 50
  end

  describe "QueueJanitor with consolidated keys" do
    test "handles consolidated queue keys" do
      user = insert(:user)
      backend = insert(:backend, user: user)
      source = insert(:source, user: user)
      pid = self()

      consolidated_key = {:consolidated, backend.id, pid}
      IngestEventQueue.upsert_tid(consolidated_key)

      events = for _ <- 1..5, do: build(:log_event, source: source)
      IngestEventQueue.add_to_table(consolidated_key, events)
      {:ok, _} = IngestEventQueue.mark_ingested(consolidated_key, events)

      assert IngestEventQueue.get_table_size(consolidated_key) == 5

      start_supervised!(
        {QueueJanitor,
         source: source,
         backend: backend,
         interval: 50,
         remainder: 0,
         consolidated: true,
         consolidated_key: {:consolidated, backend.id}}
      )

      :timer.sleep(550)
      assert IngestEventQueue.get_table_size(consolidated_key) == 0
    end

    test "uses larger max threshold for consolidated queues" do
      user = insert(:user)
      backend = insert(:backend, user: user)
      source = insert(:source, user: user)
      pid = self()

      consolidated_key = {:consolidated, backend.id, pid}
      IngestEventQueue.upsert_tid(consolidated_key)

      # 150 events exceeds base max of 100, but consolidated uses 10x multiplier
      # so effective max is 1000, and 150 events should NOT trigger a purge
      batch = for _ <- 1..150, do: build(:log_event, source: source)
      IngestEventQueue.add_to_table(consolidated_key, batch)

      assert IngestEventQueue.get_table_size(consolidated_key) == 150

      start_supervised!(
        {QueueJanitor,
         source: source,
         backend: backend,
         interval: 50,
         max: 100,
         purge_ratio: 1.0,
         consolidated: true,
         consolidated_key: {:consolidated, backend.id}}
      )

      :timer.sleep(550)
      # Events should remain because 150 < 1000 (consolidated max = 100 * 10)
      assert IngestEventQueue.get_table_size(consolidated_key) == 150
    end
  end

  describe "delete_id/2" do
    test "deletes an existing event from ETS" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      sbp = {source.id, backend.id, self()}
      IngestEventQueue.upsert_tid(sbp)
      le = build(:log_event, source: source)
      IngestEventQueue.add_to_table(sbp, [le])
      tid = IngestEventQueue.get_tid(sbp)

      assert IngestEventQueue.get_table_size(sbp) == 1
      assert :ok = IngestEventQueue.delete_id(tid, le.id)
      assert IngestEventQueue.get_table_size(sbp) == 0
    end

    test "returns :ok silently when ETS table is stale/deleted" do
      assert :ok = IngestEventQueue.delete_id(:stale_tid, "any-id")
    end
  end

  describe "update_status/3" do
    test "updates the status of an existing event" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      sbp = {source.id, backend.id, self()}
      IngestEventQueue.upsert_tid(sbp)
      le = build(:log_event, source: source)
      IngestEventQueue.add_to_table(sbp, [le])
      tid = IngestEventQueue.get_tid(sbp)

      assert :ok = IngestEventQueue.update_status(tid, le.id, :processing)
      assert [{_id, :processing, _, _, _, _}] = :ets.lookup(tid, le.id)
    end

    test "setting :pending resets the claim counter so the row is claimable again" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      sbp = {source.id, backend.id, self()}
      IngestEventQueue.upsert_tid(sbp)
      le = build(:log_event, source: source)
      IngestEventQueue.add_to_table(sbp, [le])
      tid = IngestEventQueue.get_tid(sbp)

      le_id = le.id
      assert {:ok, [{^le_id, _size}], _tid} = IngestEventQueue.take_pending_ids(sbp, 1)
      assert [{^le_id, :processing, _, _, 1, claimed_at}] = :ets.lookup(tid, le.id)
      # claiming stamps a monotonic claimed_at so stale recovery can age the row out
      assert is_integer(claimed_at)

      # returning to :pending must reset both the claim counter and claimed_at, or it stays
      # unclaimable / looks perpetually stale
      assert :ok = IngestEventQueue.update_status(tid, le.id, :pending)
      assert [{^le_id, :pending, _, _, 0, 0}] = :ets.lookup(tid, le.id)
      assert {:ok, [{^le_id, _size}], _tid} = IngestEventQueue.take_pending_ids(sbp, 1)
    end

    test "returns :ok silently when ETS table is stale/deleted" do
      assert :ok = IngestEventQueue.update_status(:stale_tid, "any-id", :processing)
    end
  end

  describe "list_processing_ids/1" do
    test "returns IDs of :processing events only" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      sbp = {source.id, backend.id, self()}
      IngestEventQueue.upsert_tid(sbp)
      le1 = build(:log_event, source: source)
      le2 = build(:log_event, source: source)
      le3 = build(:log_event, source: source)
      IngestEventQueue.add_to_table(sbp, [le1, le2, le3])
      tid = IngestEventQueue.get_tid(sbp)
      :ets.update_element(tid, le1.id, {2, :processing})
      :ets.update_element(tid, le2.id, {2, :ingested})
      # le3 stays :pending

      ids = IngestEventQueue.list_processing_ids(sbp)
      assert le1.id in ids
      refute le2.id in ids
      refute le3.id in ids
    end

    test "returns empty list when no processing events" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      sbp = {source.id, backend.id, self()}
      IngestEventQueue.upsert_tid(sbp)
      assert [] = IngestEventQueue.list_processing_ids(sbp)
    end

    test "returns empty list when queue does not exist" do
      assert [] = IngestEventQueue.list_processing_ids({0, 0, self()})
    end
  end

  describe "list_stale_processing_ids/3" do
    test "returns only :processing rows claimed at or before the cutoff" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      sbp = {source.id, backend.id, self()}
      IngestEventQueue.upsert_tid(sbp)
      [stale, fresh, pending, ingested] = for _ <- 1..4, do: build(:log_event, source: source)
      IngestEventQueue.add_to_table(sbp, [stale, fresh, pending, ingested])
      tid = IngestEventQueue.get_tid(sbp)

      now = System.monotonic_time(:millisecond)
      cutoff = now - 5_000

      # :processing, claimed before the cutoff -> stale
      :ets.update_element(tid, stale.id, [{2, :processing}, {6, now - 10_000}])
      # :processing, claimed after the cutoff -> not yet stale
      :ets.update_element(tid, fresh.id, [{2, :processing}, {6, now}])
      # :pending with an old claimed_at -> excluded by the :processing status pin, not the cutoff
      :ets.update_element(tid, pending.id, [{2, :pending}, {6, now - 10_000}])
      # :ingested with an old claimed_at -> excluded
      :ets.update_element(tid, ingested.id, [{2, :ingested}, {6, now - 10_000}])

      assert [stale_id] = IngestEventQueue.list_stale_processing_ids(sbp, cutoff, 10)
      assert stale_id == stale.id
    end

    test "bounds the number of returned IDs to the limit" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      sbp = {source.id, backend.id, self()}
      IngestEventQueue.upsert_tid(sbp)
      les = for _ <- 1..3, do: build(:log_event, source: source)
      IngestEventQueue.add_to_table(sbp, les)
      tid = IngestEventQueue.get_tid(sbp)
      now = System.monotonic_time(:millisecond)
      for le <- les, do: :ets.update_element(tid, le.id, [{2, :processing}, {6, now - 10_000}])

      assert length(IngestEventQueue.list_stale_processing_ids(sbp, now, 2)) == 2
    end

    test "returns empty list when queue does not exist" do
      assert [] = IngestEventQueue.list_stale_processing_ids({0, 0, self()}, 0, 10)
    end
  end

  describe "reset_stale_event/3 and drop_stale_event/2" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      sbp = {source.id, backend.id, self()}
      IngestEventQueue.upsert_tid(sbp)
      le = build(:log_event, source: source)
      IngestEventQueue.add_to_table(sbp, [le])
      tid = IngestEventQueue.get_tid(sbp)
      :ets.update_element(tid, le.id, [{2, :processing}, {6, 123}])
      %{tid: tid, le: le}
    end

    test "reset_stale_event/3 resets the exact row to :pending with claim/claimed_at cleared",
         %{tid: tid, le: le} do
      [row] = :ets.lookup(tid, le.id)
      assert :reset = IngestEventQueue.reset_stale_event(tid, row, %{le | retries: 1})

      le_id = le.id
      assert [{^le_id, :pending, reset_le, _size, 0, 0}] = :ets.lookup(tid, le.id)
      assert reset_le.retries == 1
    end

    test "reset_stale_event/3 skips when the stored row no longer matches", %{tid: tid, le: le} do
      [row] = :ets.lookup(tid, le.id)
      # simulate the row being re-claimed (new claimed_at) after it was observed
      :ets.update_element(tid, le.id, {6, 999})

      assert :skip = IngestEventQueue.reset_stale_event(tid, row, %{le | retries: 1})

      le_id = le.id
      assert [{^le_id, :processing, _le, _size, _claim, 999}] = :ets.lookup(tid, le.id)
    end

    test "drop_stale_event/2 deletes the exact row", %{tid: tid, le: le} do
      [row] = :ets.lookup(tid, le.id)
      assert :drop = IngestEventQueue.drop_stale_event(tid, row)
      assert [] = :ets.lookup(tid, le.id)
    end

    test "drop_stale_event/2 skips when the stored row no longer matches", %{tid: tid, le: le} do
      [row] = :ets.lookup(tid, le.id)
      :ets.update_element(tid, le.id, {6, 999})

      assert :skip = IngestEventQueue.drop_stale_event(tid, row)

      le_id = le.id
      assert [{^le_id, :processing, _le, _size, _claim, 999}] = :ets.lookup(tid, le.id)
    end
  end

  describe "total_by_status/2" do
    test "counts events by status correctly" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      sbp = {source.id, backend.id, self()}
      IngestEventQueue.upsert_tid(sbp)
      [le1, le2, le3] = for _ <- 1..3, do: build(:log_event, source: source)
      IngestEventQueue.add_to_table(sbp, [le1, le2, le3])
      tid = IngestEventQueue.get_tid(sbp)
      :ets.update_element(tid, le1.id, {2, :processing})
      :ets.update_element(tid, le2.id, {2, :ingested})

      sid_bid = {source.id, backend.id}
      assert IngestEventQueue.total_by_status(sid_bid, :pending) == 1
      assert IngestEventQueue.total_by_status(sid_bid, :processing) == 1
      assert IngestEventQueue.total_by_status(sid_bid, :ingested) == 1
    end

    test "returns 0 when no events match status" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      sbp = {source.id, backend.id, self()}
      IngestEventQueue.upsert_tid(sbp)
      sid_bid = {source.id, backend.id}
      assert IngestEventQueue.total_by_status(sid_bid, :processing) == 0
    end
  end

  describe "QueueJanitor stale :processing cleanup" do
    defp make_janitor_state(source, backend) do
      %{
        source_id: source.id,
        source_token: source.token,
        backend_id: backend.id,
        interval: 1_000,
        remainder: 100,
        max: 10_000,
        purge_ratio: 0.05,
        consolidated?: false,
        consolidated_key: nil,
        stale_processing_limit: 10_000
      }
    end

    # Force a row into :processing with a claimed_at stamp older than the staleness threshold,
    # relative to the real monotonic clock, so a single cleanup pass treats it as stuck.
    defp mark_stale(tid, id) do
      stale_at =
        System.monotonic_time(:millisecond) - QueueJanitor.stale_processing_age_ms() - 1_000

      :ets.update_element(tid, id, [{2, :processing}, {6, stale_at}])
    end

    test "fresh :processing event is not reset" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      sbp = {source.id, backend.id, self()}
      IngestEventQueue.upsert_tid(sbp)
      le = build(:log_event, source: source)
      IngestEventQueue.add_to_table(sbp, [le])
      tid = IngestEventQueue.get_tid(sbp)

      # claiming stamps claimed_at with the current monotonic time, so the row is not yet stale
      le_id = le.id
      assert {:ok, [{^le_id, _size}], _tid} = IngestEventQueue.take_pending_ids(sbp, 1)

      state = make_janitor_state(source, backend)
      QueueJanitor.do_cleanup_stale_processing(state)

      assert [{_, :processing, _, _, _, _}] = :ets.lookup(tid, le.id)
    end

    test "stale event is reset to :pending with incremented retries" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      sbp = {source.id, backend.id, self()}
      IngestEventQueue.upsert_tid(sbp)
      le = build(:log_event, source: source)
      IngestEventQueue.add_to_table(sbp, [le])
      tid = IngestEventQueue.get_tid(sbp)
      mark_stale(tid, le.id)

      state = make_janitor_state(source, backend)
      QueueJanitor.do_cleanup_stale_processing(state)

      le_id = le.id
      assert [{^le_id, :pending, reset_le, size, claim, claimed_at}] = :ets.lookup(tid, le.id)
      # retries incremented
      assert reset_le.retries == 1
      # rest of the LogEvent is intact — not corrupted by select_replace
      assert reset_le.id == le.id
      assert reset_le.body == le.body
      assert is_integer(size)
      # claim counter and claimed_at reset so the row is claimable and no longer looks stale
      assert claim == 0
      assert claimed_at == 0
    end

    test "stale event reset by the janitor can be re-claimed by take_pending_ids" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      sbp = {source.id, backend.id, self()}
      IngestEventQueue.upsert_tid(sbp)
      le = build(:log_event, source: source)
      IngestEventQueue.add_to_table(sbp, [le])
      tid = IngestEventQueue.get_tid(sbp)

      le_id = le.id
      assert {:ok, [{^le_id, _size}], _tid} = IngestEventQueue.take_pending_ids(sbp, 1)
      assert {:ok, [], _} = IngestEventQueue.take_pending_ids(sbp, 1)

      # age the claim so the stuck :processing event is detected and reset to :pending
      mark_stale(tid, le.id)
      state = make_janitor_state(source, backend)
      QueueJanitor.do_cleanup_stale_processing(state)

      # the reset event is claimable again — only true if the claim counter was reset
      assert {:ok, [{^le_id, _size}], _tid} = IngestEventQueue.take_pending_ids(sbp, 1)
    end

    test "max retries exceeded: stale event is deleted" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      sbp = {source.id, backend.id, self()}
      IngestEventQueue.upsert_tid(sbp)
      # retries already at max_stale_retries - 1
      le = build(:log_event, source: source) |> Map.put(:retries, 2)
      IngestEventQueue.add_to_table(sbp, [le])
      tid = IngestEventQueue.get_tid(sbp)
      mark_stale(tid, le.id)

      state = make_janitor_state(source, backend)
      QueueJanitor.do_cleanup_stale_processing(state)

      assert [] = :ets.lookup(tid, le.id)
    end

    test "telemetry emitted when stale events are acted on" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      sbp = {source.id, backend.id, self()}
      IngestEventQueue.upsert_tid(sbp)
      le = build(:log_event, source: source)
      IngestEventQueue.add_to_table(sbp, [le])
      tid = IngestEventQueue.get_tid(sbp)
      mark_stale(tid, le.id)

      ref = make_ref()

      :telemetry.attach(
        "test-stale-#{inspect(ref)}",
        [:logflare, :ingest_event_queue, :stale_processing],
        fn _event, measurements, _meta, pid -> send(pid, {:telemetry, measurements}) end,
        self()
      )

      state = make_janitor_state(source, backend)
      QueueJanitor.do_cleanup_stale_processing(state)

      assert_receive {:telemetry, %{reset: 1, dropped: 0}}

      :telemetry.detach("test-stale-#{inspect(ref)}")
    end

    test "no telemetry when no stale events" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      sbp = {source.id, backend.id, self()}
      IngestEventQueue.upsert_tid(sbp)

      ref = make_ref()

      :telemetry.attach(
        "test-no-stale-#{inspect(ref)}",
        [:logflare, :ingest_event_queue, :stale_processing],
        fn _event, measurements, _meta, pid -> send(pid, {:telemetry, measurements}) end,
        self()
      )

      state = make_janitor_state(source, backend)
      QueueJanitor.do_cleanup_stale_processing(state)

      refute_receive {:telemetry, _}

      :telemetry.detach("test-no-stale-#{inspect(ref)}")
    end

    test "event acked before cleanup is not resurrected to :pending" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      sbp = {source.id, backend.id, self()}
      IngestEventQueue.upsert_tid(sbp)
      le = build(:log_event, source: source)
      IngestEventQueue.add_to_table(sbp, [le])
      tid = IngestEventQueue.get_tid(sbp)
      mark_stale(tid, le.id)

      # ack lands before cleanup runs: the row leaves :processing, so it is neither selected
      # as stale nor resurrected to :pending
      IngestEventQueue.update_status(tid, le.id, :ingested)

      state = make_janitor_state(source, backend)
      QueueJanitor.do_cleanup_stale_processing(state)

      le_id = le.id
      assert [{^le_id, :ingested, _, _, _, _}] = :ets.lookup(tid, le.id)
    end

    test "telemetry reports a drop when a stale event past max retries is deleted" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      sbp = {source.id, backend.id, self()}
      IngestEventQueue.upsert_tid(sbp)
      le = build(:log_event, source: source) |> Map.put(:retries, 2)
      IngestEventQueue.add_to_table(sbp, [le])
      tid = IngestEventQueue.get_tid(sbp)
      mark_stale(tid, le.id)

      ref = make_ref()

      :telemetry.attach(
        "test-drop-#{inspect(ref)}",
        [:logflare, :ingest_event_queue, :stale_processing],
        fn _event, measurements, _meta, pid -> send(pid, {:telemetry, measurements}) end,
        self()
      )

      state = make_janitor_state(source, backend)
      QueueJanitor.do_cleanup_stale_processing(state)

      assert_receive {:telemetry, %{reset: 0, dropped: 1}}

      :telemetry.detach("test-drop-#{inspect(ref)}")
    end

    test "per-pass limit caps how many rows are reset, remainder recovered on a later pass" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      sbp = {source.id, backend.id, self()}
      IngestEventQueue.upsert_tid(sbp)
      les = for _ <- 1..3, do: build(:log_event, source: source)
      IngestEventQueue.add_to_table(sbp, les)
      tid = IngestEventQueue.get_tid(sbp)
      for le <- les, do: mark_stale(tid, le.id)

      state = make_janitor_state(source, backend) |> Map.put(:stale_processing_limit, 2)
      sid_bid = {source.id, backend.id}

      # first pass acts on at most the limit; the third stale row is left :processing
      QueueJanitor.do_cleanup_stale_processing(state)
      assert IngestEventQueue.total_by_status(sid_bid, :pending) == 2
      assert IngestEventQueue.total_by_status(sid_bid, :processing) == 1

      # the remaining stale row is recovered on the next pass
      QueueJanitor.do_cleanup_stale_processing(state)
      assert IngestEventQueue.total_by_status(sid_bid, :pending) == 3
      assert IngestEventQueue.total_by_status(sid_bid, :processing) == 0
    end
  end

  test "MapperJanitor cleans up stale tids" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)
    pid = self()
    IngestEventQueue.upsert_tid({source.id, backend.id, pid})
    tid = IngestEventQueue.get_tid({source.id, backend.id, pid})
    :ets.delete(tid)
    start_supervised!({MapperJanitor, interval: 100})
    :timer.sleep(500)
    assert IngestEventQueue.get_table_size({source.id, backend.id, pid}) == nil
    assert :ets.info(:ingest_event_queue_mapping, :size) == 0
  end

  describe "IngestEventQueue" do
    @describetag :benchmark
    @describetag timeout: :infinity
    @describetag :skip

    # Benchmark results
    # drop_with_chunking is ~15.65x slower (100 chunk)
    # select and drop is far superior.
    # There is no significant difference in ips between different chunk sizes
    # reductions for drop_with_chunking is 1.5x higher
    # memory usage for both approaches are identical
    #
    # comparison against drop using a select-key matchspec vs select-object
    # selecting the key results in a very tiny ips improvement, not significant at high table sizes.
    test "drop" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      pid = self()
      {:ok, tid} = IngestEventQueue.upsert_tid({source.id, backend.id, pid})
      sbp = {source.id, backend.id}

      Benchee.run(
        %{
          # "drop with chunking, 100 chunks" => fn {_input, to_drop} ->
          #   IngestEventQueue.drop_with_chunking(sbp, :all, to_drop, 100)
          # end,
          # "drop with chunking, 500 chunks" => fn {_input, to_drop} ->
          #   IngestEventQueue.drop_with_chunking(sbp, :all, to_drop, 500)
          # end,
          # "drop with chunking, 1k chunks" => fn {_input, to_drop} ->
          #   IngestEventQueue.drop_with_chunking(sbp, :all, to_drop, 1000)
          # end,
          # "select and drop" => fn {_input, to_drop} ->
          #   IngestEventQueue.drop(sbp, :all, to_drop, nil)
          # end,
          # "select and drop with select-key" => fn {_input, to_drop} ->
          #   IngestEventQueue.drop(sbp, :all, to_drop, :select_key)
          # end
        },
        inputs: %{
          "50k" => for(_ <- 1..50_000, do: build(:log_event, source: source)),
          "10k" => for(_ <- 1..10_000, do: build(:log_event, source: source)),
          "1k" => for(_ <- 1..1_000, do: build(:log_event, source: source))
        },
        # insert the batch
        before_scenario: fn input ->
          :ets.delete_all_objects(tid)
          IngestEventQueue.add_to_table(sbp, input)
          {input, 500}
        end,
        time: 3,
        warmup: 1,
        memory_time: 3,
        reduction_time: 3,
        print: [configuration: false],
        # use extended_statistics to view units of work done
        formatters: [{Benchee.Formatters.Console, extended_statistics: true}]
      )
    end

    @tag :benchmark
    @tag timeout: :infinity
    @tag :skip
    # benchmark results:
    # using update_element to update the element inplace is hands down superior
    # uses >30-60x less memory, effect increases as with higher batch sizes
    # >8-10.5x more ips, consistent across all batch sizes
    # reductions are the same across all 3.
    test "mark_ingested" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      pid = self()
      {:ok, tid} = IngestEventQueue.upsert_tid({source.id, backend.id, pid})
      sbp = {source.id, backend.id}

      Benchee.run(
        %{
          # "mark with :ets.insert/2 batched" => fn {input, _} ->
          #   IngestEventQueue.mark_ingested(sbp, input)
          # end,
          # "mark with :ets.insert/2 individually" => fn {input, to_drop} ->
          #   IngestEventQueue.mark_ingested_insert_individually(sbp, input)
          # end,
          # "mark with :ets.update_element/2" => fn {input, to_drop} ->
          #   IngestEventQueue.mark_ingested_update_element(sbp, input)
          # end,
        },
        inputs: %{
          "1k" => for(_ <- 1..1_000, do: build(:log_event, source: source)),
          "500" => for(_ <- 1..500, do: build(:log_event, source: source)),
          "100" => for(_ <- 1..100, do: build(:log_event, source: source)),
          "10" => for(_ <- 1..10, do: build(:log_event, source: source))
        },
        # insert the batch
        before_scenario: fn input ->
          :ets.delete_all_objects(tid)
          IngestEventQueue.add_to_table(sbp, input)
          {input, nil}
        end,
        time: 3,
        warmup: 1,
        memory_time: 3,
        reduction_time: 3,
        print: [configuration: false],
        # use extended_statistics to view units of work done
        formatters: [{Benchee.Formatters.Console, extended_statistics: true}]
      )
    end

    @tag :benchmark
    @tag timeout: :infinity
    @tag :skip
    # benchmark results
    # truncation traversal is around ~8x faster
    # memory usage for traversal is ~38x less for large queues
    # reductions  for traversals is 2x less
    test "truncate" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      pid = self()
      sbp = {source.id, backend.id, pid}
      {:ok, tid} = IngestEventQueue.upsert_tid(sbp)

      Benchee.run(
        %{
          # "truncate/3 with match_object/3, insert/2, and match_delete/3" => fn {input, _} ->
          #   IngestEventQueue.truncate_no_traversal(sbp, :all, 100)
          # end,
          "mark with :ets.select/3 and traversal" => fn {_input, _to_drop} ->
            IngestEventQueue.truncate_table(sbp, :all, 100)
          end
        },
        inputs: %{
          "50k" => for(_ <- 1..50_000, do: build(:log_event, source: source)),
          "10k" => for(_ <- 1..10_000, do: build(:log_event, source: source)),
          "1k" => for(_ <- 1..1_000, do: build(:log_event, source: source))
        },
        # insert the batch
        before_scenario: fn input ->
          :ets.delete_all_objects(tid)
          IngestEventQueue.add_to_table(sbp, input)
          {input, nil}
        end,
        time: 3,
        warmup: 1,
        memory_time: 3,
        reduction_time: 3,
        print: [configuration: false],
        # use extended_statistics to view units of work done
        formatters: [{Benchee.Formatters.Console, extended_statistics: true}]
      )
    end
  end

  # Post-benchmark results:
  # truncate is way slower at large queue sizes, ~x5 slower for 50k. 10k and 1k are comparably close for both.
  # slightly less reductions for drop
  # memory consumption is identical.
  describe "QueueJanitor" do
    @describetag :benchmark
    @describetag timeout: :infinity
    @describetag :skip

    test "truncate vs drop" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      pid = self()
      {:ok, tid} = IngestEventQueue.upsert_tid({source.id, backend.id, pid})
      sbp = {source.id, backend.id, pid}

      _state = %{
        source_id: source.id,
        backend_id: backend.id,
        remainder: 100,
        max: 500,
        purge_ratio: 0.1
      }

      Benchee.run(
        %{
          # "work - truncate" => fn {_input, _resource} ->
          #   QueueJanitor.do_work(state)
          # end,
          # "work - drop" => fn {_input, _resource} ->
          #   QueueJanitor.do_drop(state)
          # end
        },
        inputs: %{
          "50k" => for(_ <- 1..50_000, do: build(:log_event, source: source)),
          "10k" => for(_ <- 1..10_000, do: build(:log_event, source: source)),
          "1k" => for(_ <- 1..1_000, do: build(:log_event, source: source))
        },
        # insert the batch
        before_scenario: fn input ->
          :ets.delete_all_objects(tid)
          IngestEventQueue.add_to_table(sbp, input)
          {input, nil}
        end,
        time: 3,
        warmup: 1,
        # memory_time: 3,
        reduction_time: 3,
        print: [configuration: false],
        # use extended_statistics to view units of work done
        formatters: [{Benchee.Formatters.Console, extended_statistics: true}]
      )
    end
  end

  # @tag :benchmark
  # @tag timeout: :infinity
  # # @tag :skip
  # # Benchmark results
  # describe "DemandWorker" do
  #   test "fetch" do
  #     user = insert(:user)
  #     source = insert(:source, user: user)
  #     backend = insert(:backend, user: user)
  #     {:ok, tid} = IngestEventQueue.upsert_tid({source.id, backend.id, pid})
  #     sbp = {source.id, backend.id}

  #     Benchee.run(
  #       %{

  #         # "drop with chunking, 100 chunks" => fn {_input, to_drop} ->
  #         #   IngestEventQueue.drop_with_chunking(sbp, :all, to_drop, 100)
  #         # end,
  #         # "drop with chunking, 500 chunks" => fn {_input, to_drop} ->
  #         #   IngestEventQueue.drop_with_chunking(sbp, :all, to_drop, 500)
  #         # end,
  #         # "drop with chunking, 1k chunks" => fn {_input, to_drop} ->
  #         #   IngestEventQueue.drop_with_chunking(sbp, :all, to_drop, 1000)
  #         # end,
  #         # "select and drop" => fn {_input, to_drop} ->
  #         #   IngestEventQueue.drop(sbp, :all, to_drop, nil)
  #         # end,
  #         # "select and drop with select-key" => fn {_input, to_drop} ->
  #         #   IngestEventQueue.drop(sbp, :all, to_drop, :select_key)
  #         # end
  #       },
  #       inputs: %{
  #         "50k" => for(_ <- 1..50_000, do: build(:log_event)),
  #         "10k" => for(_ <- 1..10_000, do: build(:log_event)),
  #         "1k" => for(_ <- 1..1_000, do: build(:log_event))
  #       },
  #       # insert the batch
  #       before_scenario: fn input ->
  #         :ets.delete_all_objects(tid)
  #         IngestEventQueue.add_to_table(sbp, input)
  #         {input, 500}
  #       end,
  #       time: 3,
  #       warmup: 1,
  #       memory_time: 3,
  #       reduction_time: 3,
  #       print: [configuration: false],
  #       # use extended_statistics to view units of work done
  #       formatters: [{Benchee.Formatters.Console, extended_statistics: true}]
  #     )
  #   end
  # end
end
