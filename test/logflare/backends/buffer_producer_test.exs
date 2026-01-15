defmodule Logflare.Backends.BufferProducerTest do
  use Logflare.DataCase

  alias Logflare.Backends.BufferProducer
  alias Logflare.Backends.IngestEventQueue

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
    # marked as :ingested
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
end
