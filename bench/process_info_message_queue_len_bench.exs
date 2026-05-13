# Usage:
#   mix run bench/process_info_message_queue_len_bench.exs
#
# Env:
#   ITERATIONS=1000000
#   SENDERS=1
#
# Measures the cost of repeatedly calling:
#   Process.info(pid, :message_queue_len)

iterations = String.to_integer(System.get_env("ITERATIONS") || "1000000")
senders = String.to_integer(System.get_env("SENDERS") || "1")

defmodule ProcessInfoMessageQueueLenBench do
  @moduledoc false

  def sleeping_process do
    spawn_link(fn ->
      receive do
        :stop -> :ok
      after
        :infinity -> :ok
      end
    end)
  end

  def draining_process do
    spawn_link(fn -> drain_loop() end)
  end

  def send_forever(pid) do
    spawn(fn -> send_loop(pid) end)
  end

  def stop(pid) when is_pid(pid) do
    send(pid, :stop)
  end

  def bench(iterations, fun) do
    {us, result} = :timer.tc(fn -> repeat(iterations, fun, 0) end)

    %{
      total_us: us,
      ns_per_call: us * 1_000 / iterations,
      result: result
    }
  end

  defp repeat(0, _fun, acc), do: acc

  defp repeat(n, fun, acc) do
    repeat(n - 1, fun, acc + fun.())
  end

  defp drain_loop do
    receive do
      :stop -> :ok
      _ -> drain_loop()
    end
  end

  defp send_loop(pid) do
    send(pid, :msg)
    send_loop(pid)
  end
end

alias ProcessInfoMessageQueueLenBench, as: Bench

read_len = fn pid ->
  case Process.info(pid, :message_queue_len) do
    {:message_queue_len, len} -> len
    nil -> 0
  end
end

IO.puts("iterations=#{iterations}")
IO.puts("senders=#{senders}")

sleeping_pid = Bench.sleeping_process()
sleeping_result = Bench.bench(iterations, fn -> read_len.(sleeping_pid) end)
Bench.stop(sleeping_pid)

IO.puts("\nsleeping process, empty mailbox")
IO.inspect(sleeping_result)

backlogged_pid = Bench.sleeping_process()
for _ <- 1..100_000, do: send(backlogged_pid, :msg)
backlogged_result = Bench.bench(iterations, fn -> read_len.(backlogged_pid) end)
Bench.stop(backlogged_pid)

IO.puts("\nsleeping process, 100k queued messages")
IO.inspect(backlogged_result)

draining_pid = Bench.draining_process()
sender_pids = for _ <- 1..senders, do: Bench.send_forever(draining_pid)

Process.sleep(100)
draining_result = Bench.bench(iterations, fn -> read_len.(draining_pid) end)

Enum.each(sender_pids, &Process.exit(&1, :kill))
Bench.stop(draining_pid)

IO.puts("\ndraining process, concurrent sender pressure")
IO.inspect(draining_result)
