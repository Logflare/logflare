defmodule Logflare.Backends.BufferProducerTest do
  use Logflare.DataCase

  alias Logflare.Backends.BufferProducer
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.IngestEventQueue.LogEventPointer
  alias Logflare.LogEvent
  alias Logflare.PubSubRates.Cache, as: PubSubRatesCache

  import ExUnit.CaptureLog

  setup do
    insert(:plan)
    :ok
  end

  test "pulls events from IngestEventQueue" do
    user = insert(:user)
    source = insert(:source, user: user)

    le = build(:log_event, source: source)

    buffer_producer_pid =
      start_supervised!({BufferProducer, backend_id: nil, source_id: source.id})

    sid_bid_pid = {source.id, nil, buffer_producer_pid}
    :timer.sleep(100)
    :ok = IngestEventQueue.add_to_table(sid_bid_pid, [le])

    [%LogEvent{id: id}] =
      GenStage.stream([{buffer_producer_pid, max_demand: 1}])
      |> Enum.take(1)

    assert id == le.id
    assert IngestEventQueue.total_pending(sid_bid_pid) == 0
    # pop_pending/2 deletes the pointer row outright
    assert IngestEventQueue.get_table_size(sid_bid_pid) == 0
    # do_pop_key/2 doesn't record into the recent-events cache — only BigQuery's ack
    # needs deferred "recent logs" visibility, and pop_pending has already deleted the
    # generation-store row as part of claiming anyway
    assert IngestEventQueue.list_recent_events({source.id, nil}, 10) == []
  end

  test "pops events regardless of configured ingestion rate" do
    user = insert(:user)
    source = insert(:source, user: user)

    le = build(:log_event, source: source)

    buffer_producer_pid =
      start_supervised!({BufferProducer, backend_id: nil, source_id: source.id})

    sid_bid_pid = {source.id, nil, buffer_producer_pid}
    :timer.sleep(100)
    :ok = IngestEventQueue.add_to_table(sid_bid_pid, [le])

    PubSubRatesCache.cache_rates(source.token, %{
      Node.self() => %{
        average_rate: 500,
        last_rate: 500,
        max_rate: 500,
        limiter_metrics: %{
          average: 0,
          duration: 60,
          sum: 0
        }
      }
    })

    GenStage.stream([{buffer_producer_pid, max_demand: 1}])
    |> Enum.take(1)

    assert IngestEventQueue.total_pending(sid_bid_pid) == 0
    # popped regardless — no more avg-based take/pop branching
    assert IngestEventQueue.get_table_size(sid_bid_pid) == 0
  end

  test "moves events in IngestEventQueue to other queues on termination" do
    user = insert(:user)
    source = insert(:source, user: user)

    le = build(:log_event, source: source)

    buffer_producer_pid =
      start_supervised!({BufferProducer, backend_id: nil, source_id: source.id})

    sid_bid_pid = {source.id, nil, buffer_producer_pid}
    startup_table_key = {source.id, nil, nil}
    IngestEventQueue.upsert_tid(startup_table_key)
    :timer.sleep(100)
    :ok = IngestEventQueue.add_to_table(sid_bid_pid, [le])

    Process.exit(buffer_producer_pid, :normal)
    :timer.sleep(200)

    assert IngestEventQueue.total_pending(startup_table_key) == 1
    assert IngestEventQueue.total_pending(sid_bid_pid) == 0
  end

  test "returns LogEventPointer structs when id_passing is true" do
    user = insert(:user)
    source = insert(:source, user: user)

    le = build(:log_event, source: source)

    buffer_producer_pid =
      start_supervised!({BufferProducer, backend_id: nil, source_id: source.id, id_passing: true})

    sid_bid_pid = {source.id, nil, buffer_producer_pid}
    :timer.sleep(100)
    :ok = IngestEventQueue.add_to_table(sid_bid_pid, [le])

    tid = IngestEventQueue.get_tid(sid_bid_pid)

    [%LogEventPointer{id: id, queue_tid: ^tid, size: size}] =
      GenStage.stream([{buffer_producer_pid, max_demand: 1}])
      |> Enum.take(1)

    assert id == le.id
    assert is_integer(size) and size > 0
    assert IngestEventQueue.total_pending(sid_bid_pid) == 0
    # id-passing queues claim via :ets.take/2 — the pointer row is gone immediately,
    # not marked :processing in place.
    assert IngestEventQueue.get_table_size(sid_bid_pid) == 0
  end

  test "pulls events from startup queue" do
    user = insert(:user)
    source = insert(:source, user: user)

    le = build(:log_event, source: source)
    startup_key = {source.id, nil, nil}
    IngestEventQueue.upsert_tid(startup_key)
    :ok = IngestEventQueue.add_to_table(startup_key, [le])

    buffer_producer_pid =
      start_supervised!({BufferProducer, backend_id: nil, source_id: source.id})

    sid_bid_pid = {source.id, nil, buffer_producer_pid}

    GenStage.stream([{buffer_producer_pid, max_demand: 1}])
    |> Enum.take(1)

    assert IngestEventQueue.total_pending(sid_bid_pid) == 0
    # pop_pending/2 deletes the pointer row outright
    assert IngestEventQueue.get_table_size(sid_bid_pid) == 0
  end

  test "BufferProducer when discarding will display source name" do
    user = insert(:user)
    source = insert(:source, user: user)

    pid =
      start_supervised!({BufferProducer, backend_id: nil, source_id: source.id, buffer_size: 10})

    le = build(:log_event)
    items = List.duplicate(le, 100)

    captured =
      capture_log(fn ->
        send(pid, {:add_to_buffer, items})
        :timer.sleep(100)
        send(pid, {:add_to_buffer, items})
        :timer.sleep(100)
      end)

    assert captured =~ source.name
    assert captured =~ Atom.to_string(source.token)
    # log only once
    assert count_substrings(captured, source.name) == 1
  end

  def count_substrings(string, substring) do
    regex = Regex.compile!(substring)

    Regex.scan(regex, string)
    |> length()
  end

  describe "startup queue" do
    test "drains the producer's own queue before the shared startup queue" do
      user = insert(:user)
      source = insert(:source, user: user)

      # producers unconditionally copy everything sitting in startup into their own
      # queue at init — created (but left empty) before init so that copy is a no-op,
      # letting this test add both events afterward to keep them apart
      startup_key = {source.id, nil, nil}
      IngestEventQueue.upsert_tid(startup_key)

      buffer_producer_pid =
        start_supervised!({BufferProducer, backend_id: nil, source_id: source.id})

      sid_bid_pid = {source.id, nil, buffer_producer_pid}
      :timer.sleep(100)

      own_event = build(:log_event, source: source)
      :ok = IngestEventQueue.add_to_table(sid_bid_pid, [own_event])

      startup_event = build(:log_event, source: source)
      :ok = IngestEventQueue.add_to_table(startup_key, [startup_event])

      [%LogEvent{id: id}] =
        GenStage.stream([{buffer_producer_pid, max_demand: 1}])
        |> Enum.take(1)

      assert id == own_event.id
      assert IngestEventQueue.total_pending(sid_bid_pid) == 0
      # the startup-queue event is left untouched until the producer's own queue is
      # drained — startup is only pulled from to top off unmet demand
      assert IngestEventQueue.total_pending(startup_key) == 1
    end

    test "falls back to the shared startup queue once the producer's own queue is exhausted" do
      user = insert(:user)
      source = insert(:source, user: user)

      startup_key = {source.id, nil, nil}
      IngestEventQueue.upsert_tid(startup_key)

      buffer_producer_pid =
        start_supervised!({BufferProducer, backend_id: nil, source_id: source.id})

      sid_bid_pid = {source.id, nil, buffer_producer_pid}
      :timer.sleep(100)

      own_event = build(:log_event, source: source)
      :ok = IngestEventQueue.add_to_table(sid_bid_pid, [own_event])

      startup_event = build(:log_event, source: source)
      :ok = IngestEventQueue.add_to_table(startup_key, [startup_event])

      [first, second] =
        GenStage.stream([{buffer_producer_pid, max_demand: 1}])
        |> Enum.take(2)

      assert Enum.map([first, second], & &1.id) |> Enum.sort() ==
               Enum.sort([own_event.id, startup_event.id])

      assert IngestEventQueue.total_pending(sid_bid_pid) == 0
      assert IngestEventQueue.total_pending(startup_key) == 0
    end

    test "two live producers draining the same shared startup queue never claim the same pointer" do
      user = insert(:user)
      source = insert(:source, user: user)

      startup_key = {source.id, nil, nil}
      IngestEventQueue.upsert_tid(startup_key)

      # start both producers against an empty startup table first — otherwise
      # whichever producer's init-time copy-everything-from-startup runs first would
      # vacuum both events into its own queue before the second producer ever starts
      producer_a =
        start_supervised!(
          {BufferProducer, backend_id: nil, source_id: source.id, id_passing: true},
          id: :producer_a
        )

      producer_b =
        start_supervised!(
          {BufferProducer, backend_id: nil, source_id: source.id, id_passing: true},
          id: :producer_b
        )

      :timer.sleep(100)

      events = [build(:log_event, source: source), build(:log_event, source: source)]
      :ok = IngestEventQueue.add_to_table(startup_key, events)

      task_a =
        Task.async(fn -> GenStage.stream([{producer_a, max_demand: 1}]) |> Enum.take(1) end)

      task_b =
        Task.async(fn -> GenStage.stream([{producer_b, max_demand: 1}]) |> Enum.take(1) end)

      [%LogEventPointer{id: id_a}] = Task.await(task_a, 2_000)
      [%LogEventPointer{id: id_b}] = Task.await(task_b, 2_000)

      assert id_a != id_b
      assert Enum.sort([id_a, id_b]) == Enum.sort(Enum.map(events, & &1.id))
    end
  end

  describe "consolidated mode" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          user: user,
          type: :webhook,
          config: %{url: "http://example.com"}
        )

      [user: user, source: source, backend: backend]
    end

    test "batch-aware startup seeding moves complete keys and leaves partial keys", %{
      source: source,
      backend: backend
    } do
      startup_key = {:consolidated, backend.id, nil}
      IngestEventQueue.upsert_tid(startup_key)

      complete_group =
        for _ <- 1..2 do
          build(:log_event, source: source)
          |> Map.put(:event_type, :log)
          |> Map.put(:day_bucket, 12_345)
          |> Map.put(:ingest_freshness, :fresh)
        end

      partial =
        build(:log_event, source: source)
        |> Map.put(:event_type, :metric)
        |> Map.put(:day_bucket, 12_345)
        |> Map.put(:ingest_freshness, :fresh)

      :ok = IngestEventQueue.add_to_table(startup_key, complete_group ++ [partial])

      producer =
        start_supervised!(
          {BufferProducer,
           backend_id: backend.id,
           consolidated: true,
           id_passing: true,
           max_in_flight: 4,
           seed_batch_size: 2,
           interval: 5_000}
        )

      own_key = {:consolidated, backend.id, producer}

      assert IngestEventQueue.pending_batch_key_counts(own_key) == %{
               {:fresh, :log, 12_345} => 2
             }

      assert IngestEventQueue.pending_batch_key_counts(startup_key) == %{
               {:fresh, :metric, 12_345} => 1
             }
    end

    test "batch-aware startup seeding does not move aggregate-only partial groups", %{
      source: source,
      backend: backend
    } do
      startup_key = {:consolidated, backend.id, nil}
      IngestEventQueue.upsert_tid(startup_key)

      events =
        for event_type <- [:log, :metric] do
          build(:log_event, source: source)
          |> Map.put(:event_type, event_type)
          |> Map.put(:day_bucket, 12_345)
          |> Map.put(:ingest_freshness, :fresh)
        end

      :ok = IngestEventQueue.add_to_table(startup_key, events)

      producer =
        start_supervised!(
          {BufferProducer,
           backend_id: backend.id,
           consolidated: true,
           id_passing: true,
           max_in_flight: 4,
           seed_batch_size: 2,
           interval: 5_000}
        )

      own_key = {:consolidated, backend.id, producer}
      assert IngestEventQueue.total_pending(own_key) == 0
      assert IngestEventQueue.total_pending(startup_key) == 2
    end

    test "returns a raced partial seed claim to startup", %{
      source: source,
      backend: backend
    } do
      startup_key = {:consolidated, backend.id, nil}
      batch_key = {:fresh, :log, 12_345}
      IngestEventQueue.upsert_tid(startup_key)

      events =
        for _ <- 1..2 do
          build(:log_event, source: source)
          |> Map.put(:event_type, :log)
          |> Map.put(:day_bucket, 12_345)
          |> Map.put(:ingest_freshness, :fresh)
        end

      :ok = IngestEventQueue.add_to_table(startup_key, events)
      test_pid = self()

      expect(IngestEventQueue, :pending_batch_key_counts, fn ^startup_key ->
        {:ok, [raced_pointer], _tid} =
          IngestEventQueue.pop_pending_pointers_by_batch_key(startup_key, batch_key, 1)

        send(test_pid, {:raced_pointer, raced_pointer})
        %{batch_key => 2}
      end)

      producer =
        start_supervised!(
          {BufferProducer,
           backend_id: backend.id,
           consolidated: true,
           id_passing: true,
           max_in_flight: 2,
           seed_batch_size: 2,
           interval: 5_000}
        )

      own_key = {:consolidated, backend.id, producer}
      assert_receive {:raced_pointer, raced_pointer}
      assert IngestEventQueue.total_pending(own_key) == 0
      assert IngestEventQueue.total_pending(startup_key) == 1

      IngestEventQueue.reinsert_pointer(raced_pointer)
      assert IngestEventQueue.total_pending(startup_key) == 2
    end

    test "pulls events from consolidated queue", %{source: source, backend: backend} do
      startup_key = {:consolidated, backend.id, nil}
      IngestEventQueue.upsert_tid(startup_key)

      buffer_producer_pid =
        start_supervised!(
          {BufferProducer, backend_id: backend.id, consolidated: true, interval: 100}
        )

      consolidated_key = {:consolidated, backend.id, buffer_producer_pid}
      :timer.sleep(150)

      le = build(:log_event, source: source)
      :ok = IngestEventQueue.add_to_table(consolidated_key, [le])

      [event] =
        GenStage.stream([{buffer_producer_pid, max_demand: 1}])
        |> Enum.take(1)

      assert event.id == le.id
      assert IngestEventQueue.total_pending(consolidated_key) == 0
    end

    test "pulls events from consolidated startup queue", %{source: source, backend: backend} do
      startup_key = {:consolidated, backend.id, nil}
      IngestEventQueue.upsert_tid(startup_key)

      le = build(:log_event, source: source)
      :ok = IngestEventQueue.add_to_table(startup_key, [le])

      buffer_producer_pid =
        start_supervised!(
          {BufferProducer, backend_id: backend.id, consolidated: true, interval: 100}
        )

      consolidated_key = {:consolidated, backend.id, buffer_producer_pid}

      [event] =
        GenStage.stream([{buffer_producer_pid, max_demand: 1}])
        |> Enum.take(1)

      assert event.id == le.id
      assert IngestEventQueue.total_pending(consolidated_key) == 0
    end

    test "moves events back to startup queue on termination", %{source: source, backend: backend} do
      startup_key = {:consolidated, backend.id, nil}
      IngestEventQueue.upsert_tid(startup_key)

      buffer_producer_pid =
        start_supervised!(
          {BufferProducer, backend_id: backend.id, consolidated: true, interval: 100}
        )

      consolidated_key = {:consolidated, backend.id, buffer_producer_pid}
      :timer.sleep(150)

      le = build(:log_event, source: source)
      :ok = IngestEventQueue.add_to_table(consolidated_key, [le])

      Process.exit(buffer_producer_pid, :normal)
      :timer.sleep(200)

      assert IngestEventQueue.total_pending(startup_key) == 1
      assert IngestEventQueue.total_pending(consolidated_key) == 0
    end

    test "pulls LogEventPointer structs carrying routing metadata in consolidated id-passing mode",
         %{
           source: source,
           backend: backend
         } do
      startup_key = {:consolidated, backend.id, nil}
      IngestEventQueue.upsert_tid(startup_key)

      buffer_producer_pid =
        start_supervised!(
          {BufferProducer,
           backend_id: backend.id, consolidated: true, id_passing: true, interval: 100}
        )

      consolidated_key = {:consolidated, backend.id, buffer_producer_pid}
      :timer.sleep(150)

      le =
        build(:log_event, source: source)
        |> Map.put(:event_type, :trace)
        |> Map.put(:day_bucket, 123_456)
        |> Map.put(:ingest_freshness, :stale)

      :ok = IngestEventQueue.add_to_table(consolidated_key, [le])

      [pointer] =
        GenStage.stream([{buffer_producer_pid, max_demand: 1}])
        |> Enum.take(1)

      assert %LogEventPointer{
               id: event_id,
               size: size,
               event_type: :trace,
               day_bucket: 123_456,
               ingest_freshness: :stale,
               retries: 0
             } = pointer

      assert event_id == le.id
      assert size == :erlang.external_size(le.body)

      # The claimed pointer row is already deleted (pop_pending_pointers/2 claims via
      # :ets.take/2), so the full event is resolved lazily via the pointer's own tid.
      assert IngestEventQueue.lookup_event(pointer.tid, pointer.gen_event_id) == le

      assert IngestEventQueue.total_pending(consolidated_key) == 0
    end

    test "format_discarded logs backend_id for consolidated mode", %{backend: backend} do
      startup_key = {:consolidated, backend.id, nil}
      IngestEventQueue.upsert_tid(startup_key)

      pid =
        start_supervised!(
          {BufferProducer,
           backend_id: backend.id, consolidated: true, interval: 100, buffer_size: 10}
        )

      le = build(:log_event)
      items = List.duplicate(le, 100)

      captured =
        capture_log(fn ->
          send(pid, {:add_to_buffer, items})
          :timer.sleep(100)
          send(pid, {:add_to_buffer, items})
          :timer.sleep(100)
        end)

      assert captured =~ "Consolidated GenStage producer has discarded"
    end
  end

  describe "spool producer mode" do
    setup do
      on_exit(fn ->
        IngestEventQueue.delete_queue({:spool_producer, nil, nil})
      end)

      :ok
    end

    test "pulls events as LogEventPointer structs (id_passing is always true)" do
      startup_key = {:spool_producer, nil, nil}
      IngestEventQueue.upsert_tid(startup_key)

      buffer_producer_pid =
        start_supervised!({BufferProducer, spool_producer: true, interval: 100})

      spool_key = {:spool_producer, nil, buffer_producer_pid}
      :timer.sleep(150)

      le = build(:log_event)
      :ok = IngestEventQueue.add_to_table(spool_key, [le])

      tid = IngestEventQueue.get_tid(spool_key)

      [%LogEventPointer{id: id, queue_tid: ^tid, size: size}] =
        GenStage.stream([{buffer_producer_pid, max_demand: 1}])
        |> Enum.take(1)

      assert id == le.id
      assert is_integer(size) and size > 0
      assert IngestEventQueue.total_pending(spool_key) == 0
      # Claiming deletes the pointer row outright — the spool pipeline's ack/3 has
      # nothing left to clean up on the happy path; the underlying event just sits in
      # the generation store until rotation reclaims it.
      assert IngestEventQueue.get_table_size(spool_key) == 0
    end

    test "pulls events from the spool startup queue" do
      startup_key = {:spool_producer, nil, nil}
      IngestEventQueue.upsert_tid(startup_key)

      le = build(:log_event)
      :ok = IngestEventQueue.add_to_table(startup_key, [le])

      buffer_producer_pid =
        start_supervised!({BufferProducer, spool_producer: true, interval: 100})

      spool_key = {:spool_producer, nil, buffer_producer_pid}

      [%LogEventPointer{id: id}] =
        GenStage.stream([{buffer_producer_pid, max_demand: 1}])
        |> Enum.take(1)

      assert id == le.id
      assert IngestEventQueue.total_pending(spool_key) == 0
    end

    test "moves events back to the startup queue on termination" do
      startup_key = {:spool_producer, nil, nil}
      IngestEventQueue.upsert_tid(startup_key)

      buffer_producer_pid =
        start_supervised!({BufferProducer, spool_producer: true, interval: 100})

      spool_key = {:spool_producer, nil, buffer_producer_pid}
      :timer.sleep(150)

      le = build(:log_event)
      :ok = IngestEventQueue.add_to_table(spool_key, [le])

      Process.exit(buffer_producer_pid, :normal)
      :timer.sleep(200)

      assert IngestEventQueue.total_pending(startup_key) == 1
      assert IngestEventQueue.total_pending(spool_key) == 0
    end

    test "format_discarded logs a spool-specific message" do
      startup_key = {:spool_producer, nil, nil}
      IngestEventQueue.upsert_tid(startup_key)

      pid =
        start_supervised!({BufferProducer, spool_producer: true, interval: 100, buffer_size: 10})

      le = build(:log_event)
      items = List.duplicate(le, 100)

      captured =
        capture_log(fn ->
          send(pid, {:add_to_buffer, items})
          :timer.sleep(100)
          send(pid, {:add_to_buffer, items})
          :timer.sleep(100)
        end)

      assert captured =~ "Spool producer GenStage has discarded"
    end
  end

  describe "max_in_flight capping" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      [source: source, backend: backend]
    end

    # Both GenStage callbacks below are called directly rather than through a running
    # producer process — they're plain functions underneath the @impl annotation, and
    # Process.send_after(self(), ...) inside them schedules to whichever process calls
    # them, so calling them from the test process directly lets us assert on the
    # resulting timing without a full running pipeline.

    test "counts real pointer fetches against the cap and resumes after ack capacity is freed", %{
      source: source,
      backend: backend
    } do
      own_key = {:consolidated, backend.id, self()}
      startup_key = {:consolidated, backend.id, nil}
      IngestEventQueue.upsert_tid(own_key)
      IngestEventQueue.upsert_tid(startup_key)

      events = for _ <- 1..3, do: build(:log_event, source: source)
      :ok = IngestEventQueue.add_to_table(own_key, events)

      ref = :atomics.new(1, signed: true)

      state = %{
        consolidated: true,
        id_passing: true,
        demand: 0,
        source_id: nil,
        source_token: nil,
        backend_id: backend.id,
        last_discard_log_dt: nil,
        interval: 5_000,
        in_flight_ref: ref,
        max_in_flight: 2,
        timer_ref: nil
      }

      assert {:noreply, first, state} = BufferProducer.handle_demand(3, state)
      assert length(first) == 2
      assert :atomics.get(ref, 1) == 2
      assert state.demand == 1
      assert IngestEventQueue.total_pending(own_key) == 1

      :atomics.sub(ref, 1, 2)

      assert {:noreply, [last], state} = BufferProducer.handle_info(:scheduled_resolve, state)
      assert %LogEventPointer{} = last
      assert :atomics.get(ref, 1) == 1
      assert state.demand == 0
      assert IngestEventQueue.total_pending(own_key) == 0
      Process.cancel_timer(state.timer_ref)
    end

    test "handle_info(:scheduled_resolve, _) schedules a short retry when a fetch is capped",
         %{source: source, backend: backend} do
      ref = :atomics.new(1, signed: true)
      :atomics.put(ref, 1, 1)

      state = %{
        consolidated: false,
        id_passing: true,
        demand: 1,
        source_id: source.id,
        source_token: source.token,
        backend_id: backend.id,
        last_discard_log_dt: nil,
        interval: 5_000,
        in_flight_ref: ref,
        max_in_flight: 1,
        timer_ref: nil
      }

      assert {:noreply, [], _state} = BufferProducer.handle_info(:scheduled_resolve, state)

      assert_receive :scheduled_resolve, 400
    end

    test "handle_info(:scheduled_resolve, _) keeps the normal interval when genuinely empty, not capped",
         %{source: source, backend: backend} do
      ref = :atomics.new(1, signed: true)

      state = %{
        consolidated: false,
        id_passing: true,
        demand: 1,
        source_id: source.id,
        source_token: source.token,
        backend_id: backend.id,
        last_discard_log_dt: nil,
        interval: 1_000,
        in_flight_ref: ref,
        max_in_flight: 1_000,
        timer_ref: nil
      }

      assert {:noreply, [], _state} = BufferProducer.handle_info(:scheduled_resolve, state)

      refute_receive :scheduled_resolve, 400
      assert_receive :scheduled_resolve, 1_100
    end

    test "handle_demand/2 proactively schedules a short retry when capped, instead of waiting for the pending timer",
         %{source: source, backend: backend} do
      ref = :atomics.new(1, signed: true)
      :atomics.put(ref, 1, 1)

      state = %{
        consolidated: false,
        id_passing: true,
        demand: 0,
        source_id: source.id,
        source_token: source.token,
        backend_id: backend.id,
        last_discard_log_dt: nil,
        interval: 5_000,
        in_flight_ref: ref,
        max_in_flight: 1,
        timer_ref: nil
      }

      assert {:noreply, [], _state} = BufferProducer.handle_demand(1, state)

      assert_receive :scheduled_resolve, 400
    end

    test "handle_demand/2 does not schedule anything when not capped", %{
      source: source,
      backend: backend
    } do
      ref = :atomics.new(1, signed: true)

      state = %{
        consolidated: false,
        id_passing: true,
        demand: 0,
        source_id: source.id,
        source_token: source.token,
        backend_id: backend.id,
        last_discard_log_dt: nil,
        interval: 5_000,
        in_flight_ref: ref,
        max_in_flight: 1_000,
        timer_ref: nil
      }

      assert {:noreply, [], _state} = BufferProducer.handle_demand(1, state)

      refute_receive :scheduled_resolve, 400
    end

    test "handle_demand/2 cancels the previous timer instead of forking a second parallel loop when called again while still capped",
         %{source: source, backend: backend} do
      ref = :atomics.new(1, signed: true)
      :atomics.put(ref, 1, 1)

      state = %{
        consolidated: false,
        id_passing: true,
        demand: 0,
        source_id: source.id,
        source_token: source.token,
        backend_id: backend.id,
        last_discard_log_dt: nil,
        interval: 5_000,
        in_flight_ref: ref,
        max_in_flight: 1,
        timer_ref: nil
      }

      {:noreply, [], state} = BufferProducer.handle_demand(1, state)
      # Still capped — without cancelling the first timer, this would fork off a
      # second, independent :scheduled_resolve chain rather than replacing the first.
      {:noreply, [], _state} = BufferProducer.handle_demand(1, state)

      assert_receive :scheduled_resolve, 400
      refute_receive :scheduled_resolve, 400
    end
  end
end
