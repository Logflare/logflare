defmodule Logflare.Queues.MemoryBufferTest do
  @moduledoc false
  use ExUnit.Case, async: true
  alias Logflare.Buffers.MemoryBuffer

  @job %{some: "job"}

  setup do
    pid = start_supervised!(MemoryBuffer)
    {:ok, pid: pid}
  end

  test "can enqueue jobs", %{pid: pid} do
    assert :ok = MemoryBuffer.add(pid, @job)
    assert :ok = MemoryBuffer.add_many(pid, [@job])
    assert MemoryBuffer.length(pid) == 2
  end

  test "can pop n jobs from queue", %{pid: pid} do
    job1 = %{some: "job1"}
    job2 = %{some: "job2"}
    job3 = %{some: "job3"}
    job4 = %{some: "job4"}
    assert :ok = MemoryBuffer.add_many(pid, [job1, job2])
    assert :ok = MemoryBuffer.add_many(pid, [job3, job4])

    # should use fifo
    assert {:ok, [%{some: "job1"}, %{some: "job2"}]} = MemoryBuffer.pop_many(pid, 2)
    assert {:ok, [%{some: "job3"}, %{some: "job4"}]} = MemoryBuffer.pop_many(pid, 5)
    assert {:ok, []} = MemoryBuffer.pop_many(pid, 5)
  end

  test "can clear jobs from queue", %{pid: pid} do
    assert :ok = MemoryBuffer.add(pid, @job)
    MemoryBuffer.clear(pid)
    assert MemoryBuffer.length(pid) == 0
  end
end
