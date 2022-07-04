defmodule Logflare.PersistentQueueTest do
  @moduledoc false
  use ExUnit.Case, async: true
  alias Logflare.PersistentQueue

  setup do
    id = :crypto.strong_rand_bytes(5)
    pid = start_supervised!(PersistentQueue, [id])
    {:ok, pid: pid, id: id}
  end

  test "can enqueue jobs", %{pid: pid, id: id} do
    job = %{some: "job"}
    assert :ok = PersistentQueue.enqueue(pid, [job])
    assert :ok = PersistentQueue.enqueue(id, [job])
    assert PersistentQueue.length() == 2

    for arg <- [pid, id] do
      assert {:error, :not_list} = PersistentQueue.enqueue(arg, job)
    end
  end

  test "should persist data across startups", %{pid: pid, id: id} do
    job = %{some: "job"}
    assert :ok = PersistentQueue.enqueue(pid, [job])
    # kill the process
    Process.exit(pid, :kill)
    start_supervised!(PersistentQueue, [id])
    assert PersistentQueue.length() == 1
  end

  test "can pop n jobs from queue", %{id: id} do
    job1 = %{some: "job1"}
    job2 = %{some: "job2"}
    assert :ok = PersistentQueue.enqueue(id, [job1, job2])

    assert {:ok, [%{}, %{}]} = PersistentQueue.pop_many(id, 2)
    assert {:ok, []} = PersistentQueue.pop_many(id, 5)

    # kill the queue
    Process.exit(pid, :kill)
    assert {:error, :not_started} = PersistentQueue.pop_many(id, 5)
  end
end
