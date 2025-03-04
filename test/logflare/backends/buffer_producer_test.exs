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
    items = for _ <- 1..100, do: le

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
end
