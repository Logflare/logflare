defmodule Logflare.Backends.Spool.EventQueueTest do
  # EventQueue is a single named ETS table, shared across the whole node — same
  # reasoning as MemoryMonitor's test suite for why this can't run async.
  use ExUnit.Case, async: false

  alias Logflare.Backends.Spool.EventQueue

  setup do
    EventQueue.init_table()
    drain()
    :ok
  end

  defp drain do
    case EventQueue.pop(1_000) do
      [] -> :ok
      _ -> drain()
    end
  end

  defp event(body \\ %{"message" => "hello"}) do
    %Logflare.LogEvent{id: Ecto.UUID.generate(), body: body, event_type: :log}
  end

  test "push/2 is a no-op for an empty event list" do
    assert :ok = EventQueue.push([])
    assert EventQueue.count() == 0
  end

  test "push/2 returns a ref and count/0 reflects the queued chunk" do
    assert {:ok, ref} = EventQueue.push([event()])
    assert is_reference(ref)
    assert EventQueue.count() == 1
  end

  test "push/2 defaults caller_pid to nil" do
    {:ok, _ref} = EventQueue.push([event()])
    assert [chunk] = EventQueue.pop(1)
    assert chunk.caller_pid == nil
  end

  describe "push_sync/2" do
    test "is a no-op for an empty event list" do
      assert :ok = EventQueue.push_sync([])
      assert EventQueue.count() == 0
    end

    test "blocks the caller until the chunk is acked, then returns the ack result" do
      task = Task.async(fn -> EventQueue.push_sync([event()]) end)

      assert [chunk] = wait_for_chunk()
      assert chunk.caller_pid == task.pid
      send(chunk.caller_pid, {chunk.ref, :ok})

      assert Task.await(task) == :ok
    end

    test "returns the exact term the caller was acked with, including errors" do
      task = Task.async(fn -> EventQueue.push_sync([event()]) end)

      assert [chunk] = wait_for_chunk()
      send(chunk.caller_pid, {chunk.ref, {:error, :boom}})

      assert Task.await(task) == {:error, :boom}
    end

    test "gives up after timeout and returns {:error, :timeout}, leaving the chunk queued" do
      assert {:error, :timeout} = EventQueue.push_sync([event()], 10)
      assert EventQueue.count() == 1
    end
  end

  defp wait_for_chunk(attempts \\ 50) do
    case EventQueue.pop(1) do
      [] when attempts > 0 ->
        Process.sleep(10)
        wait_for_chunk(attempts - 1)

      chunks ->
        chunks
    end
  end

  test "a chunk's byte_size is the sum of its events' external body size" do
    small = event(%{"a" => 1})
    big = event(%{"a" => String.duplicate("x", 1_000)})

    {:ok, ref} = EventQueue.push([small, big])
    assert [chunk] = EventQueue.pop(1)

    assert chunk.ref == ref
    expected = :erlang.external_size(small.body) + :erlang.external_size(big.body)
    assert chunk.byte_size == expected
  end

  test "a chunk's event_count matches the number of events pushed" do
    {:ok, _ref} = EventQueue.push([event(), event(), event()])
    assert [chunk] = EventQueue.pop(1)
    assert chunk.event_count == 3
  end

  describe "get_events/1 and delete_events/1" do
    test "get_events/1 resolves the pushed events by the pointer's ref" do
      one = event(%{"i" => 1})
      two = event(%{"i" => 2})
      {:ok, ref} = EventQueue.push([one, two])

      assert EventQueue.get_events(ref) == [one, two]
    end

    test "get_events/1 returns [] for an unknown or already-deleted ref" do
      assert EventQueue.get_events(make_ref()) == []
    end

    test "delete_events/1 removes the body; a later get_events/1 returns []" do
      {:ok, ref} = EventQueue.push([event()])
      assert EventQueue.get_events(ref) != []

      :ok = EventQueue.delete_events(ref)

      assert EventQueue.get_events(ref) == []
    end

    test "requeue/1 does not delete the body — it's still resolvable after a retry" do
      {:ok, ref} = EventQueue.push([event()])
      [chunk] = EventQueue.pop(1)

      :ok = EventQueue.requeue(chunk)

      assert EventQueue.get_events(ref) != []
    end
  end

  test "pop/1 claims chunks oldest-first and removes them from the queue" do
    {:ok, ref1} = EventQueue.push([event()])
    {:ok, ref2} = EventQueue.push([event()])

    assert [chunk1, chunk2] = EventQueue.pop(2)
    assert chunk1.ref == ref1
    assert chunk2.ref == ref2
    assert EventQueue.count() == 0
  end

  test "pop/1 never splits a chunk's events across two pops" do
    events = for i <- 1..5, do: event(%{"i" => i})
    {:ok, ref} = EventQueue.push(events)

    assert [chunk] = EventQueue.pop(1)
    assert chunk.event_count == 5
    assert EventQueue.get_events(ref) == events
  end

  test "pop/1 returns fewer than requested when the queue is short" do
    {:ok, _ref} = EventQueue.push([event()])
    assert [_one] = EventQueue.pop(10)
    assert EventQueue.pop(10) == []
  end

  test "pop/1 with a non-positive count returns an empty list without touching the table" do
    {:ok, _ref} = EventQueue.push([event()])
    assert EventQueue.pop(0) == []
    assert EventQueue.count() == 1
  end

  test "requeue/1 bumps retries and keeps the same ref" do
    {:ok, ref} = EventQueue.push([event()])
    assert [chunk] = EventQueue.pop(1)
    assert chunk.retries == 0

    :ok = EventQueue.requeue(chunk)

    assert [requeued] = EventQueue.pop(1)
    assert requeued.ref == ref
    assert requeued.retries == 1
  end

  test "requeue/1 moves the chunk to the back of the queue" do
    {:ok, ref1} = EventQueue.push([event()])
    [chunk1] = EventQueue.pop(1)
    {:ok, ref2} = EventQueue.push([event()])

    :ok = EventQueue.requeue(chunk1)

    assert [first, second] = EventQueue.pop(2)
    assert first.ref == ref2
    assert second.ref == ref1
    assert second.retries == 1
  end
end
