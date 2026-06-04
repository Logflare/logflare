defmodule Logflare.Backends.BufferProducerTest do
  use Logflare.DataCase

  alias Logflare.Backends.BufferProducer
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.IngestEventQueue.QueueJanitor
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

    GenStage.stream([{buffer_producer_pid, max_demand: 1}])
    |> Enum.take(1)

    assert IngestEventQueue.total_pending(sid_bid_pid) == 0
    # marked as :ingested
    assert IngestEventQueue.get_table_size(sid_bid_pid) == 1
  end

  test "pops events when ingestion rate high" do
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
    # popped during ingest
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
    # marked ingested
    assert IngestEventQueue.get_table_size(sid_bid_pid) == 1
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

  describe "janitor overflow signaling" do
    @overflow_threshold 10

    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      [user: user, source: source, backend: backend]
    end

    test "sourec-backend pair - signals notify_overflow/2 when queue exceeds threshold", %{
      source: source,
      backend: backend
    } do
      producer_pid =
        start_supervised!(
          {BufferProducer,
           source_id: source.id,
           backend_id: backend.id,
           interval: 60_000,
           overflow_threshold: @overflow_threshold}
        )

      table_key = {source.id, backend.id, producer_pid}
      events = for _ <- 1..(@overflow_threshold + 1), do: build(:log_event, source: source)
      :ok = IngestEventQueue.add_to_table(table_key, events)

      Mimic.expect(QueueJanitor, :notify_overflow, fn sid, bid ->
        assert sid == source.id
        assert bid == backend.id
        :ok
      end)

      send(producer_pid, :scheduled_resolve)
      :timer.sleep(200)

      Mimic.verify!(QueueJanitor)
    end

    test "source-backend pair - does not signal when queue is under threshold", %{
      source: source,
      backend: backend
    } do
      producer_pid =
        start_supervised!(
          {BufferProducer,
           source_id: source.id,
           backend_id: backend.id,
           interval: 60_000,
           overflow_threshold: @overflow_threshold}
        )

      table_key = {source.id, backend.id, producer_pid}
      events = for _ <- 1..(@overflow_threshold - 1), do: build(:log_event, source: source)
      :ok = IngestEventQueue.add_to_table(table_key, events)

      reject(&QueueJanitor.notify_overflow/2)

      send(producer_pid, :scheduled_resolve)
      :timer.sleep(200)
    end

    test "producer debounce suppresses second signal within 2.5s", %{
      source: source,
      backend: backend
    } do
      producer_pid =
        start_supervised!(
          {BufferProducer,
           source_id: source.id,
           backend_id: backend.id,
           interval: 60_000,
           overflow_threshold: @overflow_threshold}
        )

      table_key = {source.id, backend.id, producer_pid}
      events = for _ <- 1..(@overflow_threshold + 1), do: build(:log_event, source: source)
      :ok = IngestEventQueue.add_to_table(table_key, events)

      # Expect exactly one call despite two resolve cycles
      expect(QueueJanitor, :notify_overflow, 1, fn _sid, _bid -> :ok end)

      send(producer_pid, :scheduled_resolve)
      :timer.sleep(50)
      send(producer_pid, :scheduled_resolve)
      :timer.sleep(200)

      Mimic.verify!(QueueJanitor)
    end

    test "producer signals again after 2.5s debounce window expires", %{
      source: source,
      backend: backend
    } do
      producer_pid =
        start_supervised!(
          {BufferProducer,
           source_id: source.id,
           backend_id: backend.id,
           interval: 60_000,
           overflow_threshold: @overflow_threshold}
        )

      table_key = {source.id, backend.id, producer_pid}
      events = for _ <- 1..(@overflow_threshold + 1), do: build(:log_event, source: source)
      :ok = IngestEventQueue.add_to_table(table_key, events)

      # Expect exactly two calls — one now, one after the window expires
      Mimic.expect(QueueJanitor, :notify_overflow, 2, fn _sid, _bid -> :ok end)

      send(producer_pid, :scheduled_resolve)
      :timer.sleep(2_600)
      send(producer_pid, :scheduled_resolve)
      :timer.sleep(200)

      Mimic.verify!(QueueJanitor)
    end

    test "consolidated path calls notify_overflow/1 with backend_id", %{backend: backend} do
      IngestEventQueue.upsert_tid({:consolidated, backend.id, nil})

      producer_pid =
        start_supervised!(
          {BufferProducer,
           backend_id: backend.id,
           consolidated: true,
           interval: 60_000,
           overflow_threshold: @overflow_threshold}
        )

      table_key = {:consolidated, backend.id, producer_pid}
      events = for _ <- 1..(@overflow_threshold + 1), do: build(:log_event)
      :ok = IngestEventQueue.add_to_table(table_key, events)

      Mimic.expect(QueueJanitor, :notify_overflow, fn bid ->
        assert bid == backend.id
        :ok
      end)

      send(producer_pid, :scheduled_resolve)
      :timer.sleep(200)

      Mimic.verify!(QueueJanitor)
    end

    test "does not signal when backend_id is nil", %{source: source} do
      producer_pid =
        start_supervised!({BufferProducer, source_id: source.id, backend_id: nil})

      table_key = {source.id, nil, producer_pid}
      events = for _ <- 1..(@overflow_threshold + 1), do: build(:log_event, source: source)
      :ok = IngestEventQueue.add_to_table(table_key, events)

      reject(&QueueJanitor.notify_overflow/2)

      send(producer_pid, :scheduled_resolve)
      :timer.sleep(200)
    end
  end
end
