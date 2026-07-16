defmodule Logflare.Backends.IngestEventQueueTest do
  use Logflare.DataCase

  alias Logflare.LogEvent
  alias Logflare.PubSubRates
  alias Logflare.Backends.IngestEventQueue.BufferCacheWorker
  alias Logflare.Backends.IngestEventQueue.QueueJanitor
  alias Logflare.Backends.IngestEventQueue.MapperJanitor
  alias Logflare.Backends.IngestEventQueue.GenerationJanitor
  alias Logflare.Backends.IngestEventQueue.LogEventPointer
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

      {:ok, [^le]} = IngestEventQueue.pop_pending(key, 1)
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
      assert {:ok, [^le]} = IngestEventQueue.pop_pending(key, 1)
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
      queues_key = Tuple.delete_at(sbp, 2)

      # insert to table
      assert :ok = IngestEventQueue.add_to_table(sbp, [le])
      assert IngestEventQueue.get_table_size(sbp) == 1

      # pop_pending_pointers/2 claims the pointer only, leaving the generation-store
      # row in place (unlike pop_pending/2 — see BigQuery.Pipeline's ack)
      assert {:ok, [pointer], _tid} = IngestEventQueue.pop_pending_pointers(sbp, 5)
      assert IngestEventQueue.total_pending(sbp) == 0

      # deferring deletion via a recent-events pointer (BigQuery's ack pattern) keeps
      # the event resolvable a bit longer instead of deleting it immediately
      IngestEventQueue.record_recent_pointer(queues_key, pointer.id, pointer.tid)
      assert IngestEventQueue.list_recent_events(queues_key, 10) == [le]

      # truncate_recent/2 bounds it per queue, same job QueueJanitor does on a
      # schedule, and also deletes the now-evicted pointer's underlying event
      assert :ok = IngestEventQueue.truncate_recent(queues_key, 0)
      assert IngestEventQueue.list_recent_events(queues_key, 10) == []
      assert IngestEventQueue.lookup_event(pointer.tid, pointer.id) == nil
    end

    test "drop n items from a queue", %{source: source, source_backend_pid: sbp} do
      batch = for _ <- 1..500, do: build(:log_event, source: source)
      assert :ok = IngestEventQueue.add_to_table(sbp, batch)

      queues_key = Tuple.delete_at(sbp, 2)
      gen_tid = IngestEventQueue.current_generation_tid(queues_key)
      pointer_tid = IngestEventQueue.get_tid(sbp)
      ids_before = pointer_tid |> :ets.tab2list() |> Enum.map(&elem(&1, 0)) |> MapSet.new()

      assert {:ok, 2} = IngestEventQueue.drop_pending(sbp, 2)
      assert IngestEventQueue.get_table_size(sbp) == 498

      ids_after = pointer_tid |> :ets.tab2list() |> Enum.map(&elem(&1, 0)) |> MapSet.new()
      dropped_ids = MapSet.difference(ids_before, ids_after)
      assert MapSet.size(dropped_ids) == 2

      # load-shedding must free the actual event body too, not just the pointer row —
      # otherwise the memory it's meant to relieve sits until GenerationJanitor's next
      # rotation instead of being freed immediately
      for id <- dropped_ids do
        assert IngestEventQueue.lookup_event(gen_tid, id) == nil
      end

      remaining_id = ids_after |> MapSet.to_list() |> hd()
      assert %LogEvent{} = IngestEventQueue.lookup_event(gen_tid, remaining_id)
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

  describe "pop_pending_pointers/2" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      sbp = {source.id, insert(:backend, user: user).id, self()}
      IngestEventQueue.upsert_tid(sbp)
      [source: source, sbp: sbp]
    end

    test "claims pending events, returning LogEventPointer structs", %{
      source: source,
      sbp: sbp
    } do
      events = for _ <- 1..5, do: build(:log_event, source: source)
      assert :ok = IngestEventQueue.add_to_table(sbp, events)

      assert {:ok, pointers, tid} = IngestEventQueue.pop_pending_pointers(sbp, 5)
      assert tid != nil
      assert length(pointers) == 5
      assert Enum.all?(pointers, &match?(%LogEventPointer{}, &1))

      taken_ids = for %LogEventPointer{id: id} <- pointers, do: id
      assert Enum.sort(taken_ids) == Enum.sort(for e <- events, do: e.id)
      assert IngestEventQueue.total_pending(sbp) == 0
    end

    test "works with consolidated keys", %{source: source} do
      backend = insert(:backend, user: insert(:user))
      key = {:consolidated, backend.id, self()}
      IngestEventQueue.upsert_tid(key)
      le = build(:log_event, source: source)

      assert :ok = IngestEventQueue.add_to_table(key, [le])
      assert {:ok, [pointer], _tid} = IngestEventQueue.pop_pending_pointers(key, 1)
      assert pointer.id == le.id
      assert IngestEventQueue.total_pending(key) == 0
    end

    test "pointers carry routing metadata and can resolve the full event via lookup_event/2",
         %{source: source, sbp: sbp} do
      fresh =
        build(:log_event, source: source)
        |> Map.put(:event_type, :log)
        |> Map.put(:day_bucket, 12_345)
        |> Map.put(:ingest_freshness, :fresh)

      stale =
        build(:log_event, source: source)
        |> Map.put(:event_type, :trace)
        |> Map.put(:day_bucket, 54_321)
        |> Map.put(:ingest_freshness, :stale)

      assert :ok = IngestEventQueue.add_to_table(sbp, [fresh, stale])

      assert {:ok, pointers, tid} = IngestEventQueue.pop_pending_pointers(sbp, 2)
      assert tid != nil

      by_id = Map.new(pointers, &{&1.id, &1})

      fresh_pointer = Map.fetch!(by_id, fresh.id)
      assert fresh_pointer.event_type == :log
      assert fresh_pointer.day_bucket == 12_345
      assert fresh_pointer.ingest_freshness == :fresh
      assert fresh_pointer.size == :erlang.external_size(fresh.body)
      assert IngestEventQueue.lookup_event(fresh_pointer.tid, fresh_pointer.id).id == fresh.id

      stale_pointer = Map.fetch!(by_id, stale.id)
      assert stale_pointer.event_type == :trace
      assert stale_pointer.day_bucket == 54_321
      assert stale_pointer.ingest_freshness == :stale

      assert IngestEventQueue.total_pending(sbp) == 0
    end

    test "returns empty without claiming when count is 0", %{
      source: source,
      sbp: sbp
    } do
      assert :ok = IngestEventQueue.add_to_table(sbp, [build(:log_event, source: source)])

      assert {:ok, [], nil} = IngestEventQueue.pop_pending_pointers(sbp, 0)
      assert IngestEventQueue.total_pending(sbp) == 1
    end

    test "returns :not_initialized for an unknown table" do
      assert {:error, :not_initialized} =
               IngestEventQueue.pop_pending_pointers({-1, -1, self()}, 5)
    end

    test "claims rows with nil routing fields and returns nils on the pointer", %{
      source: source,
      sbp: sbp
    } do
      event =
        build(:log_event, source: source)
        |> Map.put(:event_type, nil)
        |> Map.put(:day_bucket, nil)
        |> Map.put(:ingest_freshness, nil)

      assert :ok = IngestEventQueue.add_to_table(sbp, [event])

      assert {:ok, [pointer], tid} = IngestEventQueue.pop_pending_pointers(sbp, 1)
      assert tid != nil
      assert pointer.id == event.id
      assert pointer.size == :erlang.external_size(event.body)
      assert pointer.event_type == nil
      assert pointer.day_bucket == nil
      assert pointer.ingest_freshness == nil
      assert IngestEventQueue.total_pending(sbp) == 0
    end

    test "respects the requested count and leaves the remainder pending", %{
      source: source,
      sbp: sbp
    } do
      events = for _ <- 1..10, do: build(:log_event, source: source)
      assert :ok = IngestEventQueue.add_to_table(sbp, events)

      assert {:ok, pointers, _tid} = IngestEventQueue.pop_pending_pointers(sbp, 4)
      assert length(pointers) == 4
      assert IngestEventQueue.total_pending(sbp) == 6
    end

    test "does not re-claim events already taken across sequential calls", %{
      source: source,
      sbp: sbp
    } do
      events = for _ <- 1..10, do: build(:log_event, source: source)
      assert :ok = IngestEventQueue.add_to_table(sbp, events)

      assert {:ok, first, _} = IngestEventQueue.pop_pending_pointers(sbp, 4)
      assert {:ok, second, _} = IngestEventQueue.pop_pending_pointers(sbp, 10)

      first_ids = MapSet.new(first, & &1.id)
      second_ids = MapSet.new(second, & &1.id)

      assert MapSet.disjoint?(first_ids, second_ids)
      assert MapSet.size(first_ids) == 4
      assert MapSet.size(second_ids) == 6
    end

    test "an event can be re-claimed after being re-added to the queue", %{
      source: source,
      sbp: sbp
    } do
      [event] = events = [build(:log_event, source: source)]
      assert :ok = IngestEventQueue.add_to_table(sbp, events)
      event_id = event.id

      assert {:ok, [%LogEventPointer{id: ^event_id}], _tid} =
               IngestEventQueue.pop_pending_pointers(sbp, 1)

      assert {:ok, [], _} = IngestEventQueue.pop_pending_pointers(sbp, 1)

      # re-adding (e.g. the BigQuery/ClickHouse requeue path) makes it claimable again
      assert :ok = IngestEventQueue.add_to_table(sbp, events)

      assert {:ok, [%LogEventPointer{id: ^event_id}], _tid} =
               IngestEventQueue.pop_pending_pointers(sbp, 1)
    end

    # Regression guard for the atomic claim. For correct code this passes
    # deterministically — :ets.take/2 is the sole claim primitive, so no event is ever
    # taken twice regardless of scheduling. The large batch and several concurrent
    # claimers exist to make a *regression* actually hit the race window and fail,
    # rather than slip through. Tagged :race so it can be isolated, but it is safe to
    # run by default.
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
            {:ok, pointers, _tid} = IngestEventQueue.pop_pending_pointers(sbp, count)
            for %LogEventPointer{id: id} <- pointers, do: id
          end)
        end
        |> Task.await_many(10_000)
        |> List.flatten()

      assert length(claimed) == length(Enum.uniq(claimed))
      assert length(claimed) == count
      assert IngestEventQueue.total_pending(sbp) == 0
    end
  end

  describe "reinsert_pointer/1" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      sbp = {source.id, insert(:backend, user: user).id, self()}
      IngestEventQueue.upsert_tid(sbp)
      [source: source, sbp: sbp]
    end

    test "reinserts the pointer directly into the queue it was claimed from", %{
      source: source,
      sbp: sbp
    } do
      le = build(:log_event, source: source)
      assert :ok = IngestEventQueue.add_to_table(sbp, [le])

      assert {:ok, [pointer], _tid} = IngestEventQueue.pop_pending_pointers(sbp, 1)
      assert IngestEventQueue.total_pending(sbp) == 0

      assert :ok = IngestEventQueue.reinsert_pointer(%{pointer | retries: pointer.retries + 1})
      assert IngestEventQueue.total_pending(sbp) == 1

      assert {:ok, [reclaimed], _tid} = IngestEventQueue.pop_pending_pointers(sbp, 1)
      assert reclaimed.id == le.id
      assert reclaimed.retries == 1
    end

    test "is a silent no-op when the queue table is stale" do
      tid = :ets.new(:stale_reinsert_queue, [:public, :set])
      :ets.delete(tid)

      pointer = %LogEventPointer{
        id: "some-id",
        tid: tid,
        queue_tid: tid,
        size: 0,
        retries: 0,
        event_type: :log,
        day_bucket: 0,
        ingest_freshness: :fresh
      }

      assert :ok = IngestEventQueue.reinsert_pointer(pointer)
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

    test "routes an entire incoming batch to the startup queue when every queue is at the hard cap",
         %{source: source, backend: backend} do
      startup_key = {source.id, backend.id, nil}
      IngestEventQueue.upsert_tid(startup_key)

      max_size = IngestEventQueue.max_queue_size()

      queues =
        for n <- 1..3 do
          queue = {source.id, backend.id, :erlang.list_to_pid(~c"<0.200.#{n}>")}
          IngestEventQueue.upsert_tid(queue)
          :ok = IngestEventQueue.add_to_table(queue, build_list(max_size, :log_event))
          queue
        end

      # every existing queue is at the hard cap — nothing eligible remains, so the
      # whole new batch falls through to the startup queue instead
      new_events = build_list(10_000, :log_event)
      :ok = IngestEventQueue.add_to_table({source.id, backend.id}, new_events)

      assert IngestEventQueue.get_table_size(startup_key) == 10_000
      for queue <- queues, do: assert(IngestEventQueue.get_table_size(queue) == max_size)
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

    test "weights round-robin toward the less-loaded consolidated queue once the spread exceeds the noise floor",
         %{backend: backend} do
      pid1 = :erlang.list_to_pid(~c"<0.100.1>")
      pid2 = :erlang.list_to_pid(~c"<0.100.2>")
      loaded_queue = {:consolidated, backend.id, pid1}
      empty_queue = {:consolidated, backend.id, pid2}

      IngestEventQueue.upsert_tid(loaded_queue)
      IngestEventQueue.upsert_tid(empty_queue)

      # pre-load one queue well past the noise floor (chunk_size, 100 here) so
      # weight_by_load/2 kicks in instead of falling back to plain round-robin
      :ok = IngestEventQueue.add_to_table(loaded_queue, build_list(40_000, :log_event))

      new_events = build_list(6_000, :log_event)

      :ok =
        IngestEventQueue.add_to_table({:consolidated, backend.id}, new_events, chunk_size: 50)

      loaded_added = IngestEventQueue.get_table_size(loaded_queue) - 40_000
      empty_added = IngestEventQueue.get_table_size(empty_queue)

      assert loaded_added + empty_added == 6_000
      assert empty_added > loaded_added * 3
    end

    test "falls back to plain round-robin for consolidated queues when the spread is within the noise floor",
         %{backend: backend} do
      pid1 = :erlang.list_to_pid(~c"<0.100.1>")
      pid2 = :erlang.list_to_pid(~c"<0.100.2>")
      queue1 = {:consolidated, backend.id, pid1}
      queue2 = {:consolidated, backend.id, pid2}

      IngestEventQueue.upsert_tid(queue1)
      IngestEventQueue.upsert_tid(queue2)

      # a 10-event difference is far below the noise floor (chunk_size, 100 here)
      :ok = IngestEventQueue.add_to_table(queue1, build_list(1_000, :log_event))
      :ok = IngestEventQueue.add_to_table(queue2, build_list(1_010, :log_event))

      new_events = build_list(200, :log_event)

      :ok =
        IngestEventQueue.add_to_table({:consolidated, backend.id}, new_events, chunk_size: 50)

      added1 = IngestEventQueue.get_table_size(queue1) - 1_000
      added2 = IngestEventQueue.get_table_size(queue2) - 1_010

      assert added1 == 100
      assert added2 == 100
    end

    test "routes an entire incoming batch to the startup queue when every consolidated queue is at the hard cap",
         %{backend: backend} do
      startup_key = {:consolidated, backend.id, nil}
      IngestEventQueue.upsert_tid(startup_key)

      max_size = IngestEventQueue.max_consolidated_queue_size()

      queues =
        for n <- 1..3 do
          queue = {:consolidated, backend.id, :erlang.list_to_pid(~c"<0.200.#{n}>")}
          IngestEventQueue.upsert_tid(queue)
          :ok = IngestEventQueue.add_to_table(queue, build_list(max_size, :log_event))
          queue
        end

      new_events = build_list(10_000, :log_event)
      :ok = IngestEventQueue.add_to_table({:consolidated, backend.id}, new_events)

      assert IngestEventQueue.get_table_size(startup_key) == 10_000
      for queue <- queues, do: assert(IngestEventQueue.get_table_size(queue) == max_size)
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

    # Regression guard for concurrent access to a shared queue (e.g. BufferProducer's
    # startup-queue prioritization, where more than one live producer reads the same
    # table). The candidate :ets.select/3 is not atomic, but the resolution that
    # actually hands back an event — :ets.take/2 on the generation store — is: a
    # duplicate-selected id resolves to a real event for exactly one racer and nil
    # (filtered out) for every other. Tagged :race so it can be isolated, but it is
    # safe to run by default.
    @tag :race
    test "concurrent claims on the same queue never pop an event twice", %{
      key: key,
      source: source
    } do
      count = 1_000
      events = for _ <- 1..count, do: build(:log_event, source: source)
      assert :ok = IngestEventQueue.add_to_table(key, events)

      claimed =
        for _ <- 1..8 do
          Task.async(fn ->
            {:ok, popped} = IngestEventQueue.pop_pending(key, count)
            Enum.map(popped, & &1.id)
          end)
        end
        |> Task.await_many(10_000)
        |> List.flatten()

      assert length(claimed) == length(Enum.uniq(claimed))
      assert length(claimed) == count
      assert IngestEventQueue.get_table_size(key) == 0
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
      {:ok, pointers, _tid} = IngestEventQueue.pop_pending_pointers(consolidated_key, 5)
      queues_key = {:consolidated, backend.id}

      Enum.each(pointers, fn pointer ->
        IngestEventQueue.record_recent_pointer(queues_key, pointer.id, pointer.tid)
      end)

      # pop_pending_pointers/2 deletes the pointer row outright (there's no lingering
      # :ingested status for the janitor to purge later) — the recent-events cache
      # is what QueueJanitor now bounds instead, via truncate_recent/2.
      assert IngestEventQueue.get_table_size(consolidated_key) == 0
      assert length(IngestEventQueue.list_recent_events(queues_key, 10)) == 5

      start_supervised!(
        {QueueJanitor,
         source: source,
         backend: backend,
         interval: 50,
         remainder: 0,
         consolidated: true,
         consolidated_key: queues_key}
      )

      :timer.sleep(550)
      assert IngestEventQueue.get_table_size(consolidated_key) == 0
      assert IngestEventQueue.list_recent_events(queues_key, 10) == []
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

  describe "total_by_status/2" do
    test "pending equals table size; processing/ingested are always 0" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      sbp = {source.id, backend.id, self()}
      IngestEventQueue.upsert_tid(sbp)
      [le1, le2, le3] = for _ <- 1..3, do: build(:log_event, source: source)
      IngestEventQueue.add_to_table(sbp, [le1, le2, le3])

      sid_bid = {source.id, backend.id}
      assert IngestEventQueue.total_by_status(sid_bid, :pending) == 3
      assert IngestEventQueue.total_by_status(sid_bid, :processing) == 0
      assert IngestEventQueue.total_by_status(sid_bid, :ingested) == 0
    end

    test "returns 0 when no events match status" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      sbp = {source.id, backend.id, self()}
      IngestEventQueue.upsert_tid(sbp)
      sid_bid = {source.id, backend.id}
      assert IngestEventQueue.total_by_status(sid_bid, :pending) == 0
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

  describe "recent-events cache" do
    # Records a real event into `key`'s table and defers its deletion via a
    # recent-events pointer (BigQuery ack's pattern) so tests below can exercise
    # list_recent_events/2, sweep_recent_events/1, truncate_recent/2 against a real,
    # resolvable pointer instead of a bare LogEvent (the cache only ever stores
    # pointers now — see record_recent_pointer/3).
    defp seed_recent_pointer(queues_key) do
      key = Tuple.insert_at(queues_key, tuple_size(queues_key), self())
      IngestEventQueue.upsert_tid(key)
      le = build(:log_event)
      IngestEventQueue.add_to_table(key, [le])
      {:ok, [pointer], _tid} = IngestEventQueue.pop_pending_pointers(key, 1)
      IngestEventQueue.record_recent_pointer(queues_key, pointer.id, pointer.tid)
      le
    end

    test "record_recent_pointer/3 + list_recent_events/2 round-trip, most recent first" do
      queues_key = {:consolidated, System.unique_integer([:positive])}
      le1 = seed_recent_pointer(queues_key)
      le2 = seed_recent_pointer(queues_key)

      assert IngestEventQueue.list_recent_events(queues_key, 10) == [le2, le1]
    end

    test "list_recent_events/2 respects the requested count" do
      queues_key = {:consolidated, System.unique_integer([:positive])}
      for _ <- 1..5, do: seed_recent_pointer(queues_key)

      assert length(IngestEventQueue.list_recent_events(queues_key, 2)) == 2
    end

    test "list_recent_events/2 only returns events for the given queues_key" do
      key1 = {:consolidated, System.unique_integer([:positive])}
      key2 = {:consolidated, System.unique_integer([:positive])}
      le1 = seed_recent_pointer(key1)
      le2 = seed_recent_pointer(key2)

      assert IngestEventQueue.list_recent_events(key1, 10) == [le1]
      assert IngestEventQueue.list_recent_events(key2, 10) == [le2]
    end

    test "sweep_recent_events/1 drops rows older than max_age_ms" do
      queues_key = {:consolidated, System.unique_integer([:positive])}
      seed_recent_pointer(queues_key)

      :timer.sleep(20)

      assert :ok = IngestEventQueue.sweep_recent_events(10)
      assert IngestEventQueue.list_recent_events(queues_key, 10) == []
    end

    test "sweep_recent_events/1 keeps rows younger than max_age_ms" do
      queues_key = {:consolidated, System.unique_integer([:positive])}
      le = seed_recent_pointer(queues_key)

      assert :ok = IngestEventQueue.sweep_recent_events(:timer.minutes(5))
      assert IngestEventQueue.list_recent_events(queues_key, 10) == [le]
    end

    test "record_recent_pointer/3 + list_recent_events/2 resolves the pointer to its event" do
      key = {:consolidated, System.unique_integer([:positive]), self()}
      queues_key = {:consolidated, elem(key, 1)}
      IngestEventQueue.upsert_tid(key)
      le = build(:log_event)
      IngestEventQueue.add_to_table(key, [le])

      {:ok, [pointer], _tid} = IngestEventQueue.pop_pending_pointers(key, 1)

      assert :ok = IngestEventQueue.record_recent_pointer(queues_key, pointer.id, pointer.tid)
      # the generation-store row is deliberately still there, deletion deferred
      assert IngestEventQueue.lookup_event(pointer.tid, pointer.id) == le
      assert IngestEventQueue.list_recent_events(queues_key, 10) == [le]
    end

    test "list_recent_events/2 drops a pointer whose generation-store row is already gone" do
      queues_key = {:consolidated, System.unique_integer([:positive])}
      gen_tid = :ets.new(:fake_generation, [:set, :public])

      assert :ok = IngestEventQueue.record_recent_pointer(queues_key, "missing-id", gen_tid)
      assert IngestEventQueue.list_recent_events(queues_key, 10) == []
    end

    test "truncate_recent/2 deletes an evicted pointer's referenced generation-store event too" do
      key = {:consolidated, System.unique_integer([:positive]), self()}
      queues_key = {:consolidated, elem(key, 1)}
      IngestEventQueue.upsert_tid(key)
      le = build(:log_event)
      IngestEventQueue.add_to_table(key, [le])

      {:ok, [pointer], _tid} = IngestEventQueue.pop_pending_pointers(key, 1)
      IngestEventQueue.record_recent_pointer(queues_key, pointer.id, pointer.tid)

      assert :ok = IngestEventQueue.truncate_recent(queues_key, 0)
      assert IngestEventQueue.list_recent_events(queues_key, 10) == []
      assert IngestEventQueue.lookup_event(pointer.tid, pointer.id) == nil
    end

    test "sweep_recent_events/1 deletes an evicted pointer's referenced generation-store event too" do
      key = {:consolidated, System.unique_integer([:positive]), self()}
      queues_key = {:consolidated, elem(key, 1)}
      IngestEventQueue.upsert_tid(key)
      le = build(:log_event)
      IngestEventQueue.add_to_table(key, [le])

      {:ok, [pointer], _tid} = IngestEventQueue.pop_pending_pointers(key, 1)
      IngestEventQueue.record_recent_pointer(queues_key, pointer.id, pointer.tid)

      :timer.sleep(20)

      assert :ok = IngestEventQueue.sweep_recent_events(10)
      assert IngestEventQueue.list_recent_events(queues_key, 10) == []
      assert IngestEventQueue.lookup_event(pointer.tid, pointer.id) == nil
    end
  end

  describe "GenerationJanitor" do
    test "drops generations older than max_age_ms, leaving claims from that generation unresolvable" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      pid = self()

      consolidated_key = {:consolidated, backend.id, pid}
      IngestEventQueue.upsert_tid(consolidated_key)

      le = build(:log_event, source: source)
      :ok = IngestEventQueue.add_to_table(consolidated_key, [le])

      queues_key = {:consolidated, backend.id}
      assert [{gen_tid, _created_at}] = IngestEventQueue.list_generations(queues_key)
      assert :ets.info(gen_tid, :size) == 1

      # GenerationJanitor does a GLOBAL sweep (list_generation_queues_keys/0) every
      # tick, batching every discovered key into one new_generations/1 call and then
      # checking each for staleness — and IngestEventQueue's generation table is a
      # singleton shared across the whole test suite run, never reset between tests.
      # Under a full suite run (even just this file, since tests here run
      # sequentially) this set can easily reach several hundred entries by the time
      # this test runs, so a single tick's batch can take meaningfully longer than it
      # does in isolation. max_age_ms needs real margin above that, or a generation
      # can look "aged" and get dropped within the very same tick that created it —
      # 10ms was fine when creation and its own staleness check happened back-to-back
      # per key, but not once hundreds of other keys' work can land in between.
      start_supervised!({GenerationJanitor, interval: 50, max_age_ms: 1_000})

      TestUtils.retry_assert(fn ->
        # the dropped generation's table is gone outright (O(1) whole-table delete,
        # not a per-row scan)
        assert :ets.info(gen_tid, :name) == :undefined
      end)

      # claiming the still-present pointer now resolves to a dead generation — the same
      # "not found" outcome as any other lookup miss, not a crash
      assert {:ok, [%LogEventPointer{id: claimed_id, tid: claimed_tid}], _tid} =
               IngestEventQueue.pop_pending_pointers(consolidated_key, 1)

      assert claimed_id == le.id
      assert claimed_tid == gen_tid
      assert IngestEventQueue.lookup_event(claimed_tid, le.id) == nil

      # rotation keeps producing fresh generations for a queues_key with live traffic
      TestUtils.retry_assert(fn ->
        assert [_ | _] = IngestEventQueue.list_generations(queues_key)
      end)
    end

    test "do_rotate/1 is a no-op for a queues_key with no generations" do
      assert :ok = GenerationJanitor.do_rotate(%{interval: 50, max_age_ms: 10})
    end
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
        %{},
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
        %{},
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
end
