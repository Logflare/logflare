defmodule Logflare.Backends.BufferProducerTest do
  use Logflare.DataCase

  alias Logflare.Backends.BufferProducer
  alias Logflare.Backends.IngestEventQueue

  import ExUnit.CaptureLog

  test "pulls events from IngestEventQueue via IngestEventQueueDemandWorker" do
    user = insert(:user)
    source = insert(:source, user: user)

    IngestEventQueue.upsert_tid({source, nil})
    le = build(:log_event, source: source)
    IngestEventQueue.add_to_table({source, nil}, [le])

    start_supervised!({IngestEventQueue.DemandWorker, source: source})
    pid = start_supervised!({BufferProducer, backend: nil, source: source})
    :timer.sleep(100)

    GenStage.stream([{pid, max_demand: 1}])
    |> Enum.take(1)

    assert IngestEventQueue.count_pending({source, nil}) == 0
    # marked as :ingested
    assert IngestEventQueue.get_table_size({source, nil}) == 1
  end

  test "BufferProducer when discarding will display source name" do
    user = insert(:user)
    source = insert(:source, user: user)

    pid =
      start_supervised!({BufferProducer, backend: nil, source: source, buffer_size: 10})

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
