defmodule Logflare.Queues.MemoryBufferTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Logflare.Buffers.Buffer

  @subject Logflare.Buffers.MemoryBuffer

  @job %{some: "job"}

  setup do
    pid = start_supervised!(@subject)
    {:ok, pid: pid}
  end

  test "can enqueue jobs", %{pid: pid} do
    assert :ok = Buffer.add(@subject, pid, @job)
    assert :ok = Buffer.add_many(@subject, pid, [@job])
    assert Buffer.length(@subject, pid) == 2
  end

  test "can pop n jobs from queue", %{pid: pid} do
    job1 = %{some: "job1"}
    job2 = %{some: "job2"}
    job3 = %{some: "job3"}
    job4 = %{some: "job4"}
    assert :ok = Buffer.add_many(@subject, pid, [job1, job2])
    assert :ok = Buffer.add_many(@subject, pid, [job3, job4])

    # should use fifo
    assert {:ok, [%{some: "job1"}, %{some: "job2"}]} = Buffer.pop_many(@subject, pid, 2)
    assert {:ok, [%{some: "job3"}, %{some: "job4"}]} = Buffer.pop_many(@subject, pid, 5)
    assert {:ok, []} = Buffer.pop_many(@subject, pid, 5)
  end

  test "can clear jobs from queue", %{pid: pid} do
    assert :ok = Buffer.add(@subject, pid, @job)
    Buffer.clear(@subject, pid)
    assert Buffer.length(@subject, pid) == 0
  end
end
