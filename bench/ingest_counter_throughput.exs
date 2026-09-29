# Normal-VM microbenchmark of the source counter update path. The ETS and
# sharded modes reproduce the prior primitives; hybrid calls the actual module.
defmodule Logflare.Bench.IngestCounterThroughput do
  @moduledoc false

  alias Logflare.Sources.Counters

  def run do
    {:ok, _pid} = Counters.start_link()
    IO.puts("OTP=#{:erlang.system_info(:otp_release)} schedulers=#{System.schedulers_online()}")

    for workers <- [1, 2, 8], mode <- [:ets, :sharded, :hybrid] do
      iterations = if workers == 1, do: 1_000_000, else: 50_000
      samples = for trial <- 1..3, do: sample(workers, iterations, mode, trial)
      median = samples |> Enum.sort() |> Enum.at(1)

      IO.puts(
        "workers=#{workers} mode=#{mode} median_ops_s=#{round(median)} " <>
          "runs=#{inspect(Enum.map(samples, &round/1))}"
      )
    end
  end

  defp sample(workers, iterations, mode, trial) do
    key = String.to_atom("lcnt_bench_#{mode}_#{workers}_#{trial}")
    {increment, verify, cleanup} = prepare(mode, key, workers * iterations)
    barrier = :atomics.new(2, signed: false)

    tasks =
      for _ <- 1..workers do
        Task.async(fn ->
          :atomics.add_get(barrier, 1, 1)
          wait(barrier, 2, 1)
          repeat(increment, iterations)
        end)
      end

    wait(barrier, 1, workers)
    started = System.monotonic_time()
    :atomics.put(barrier, 2, 1)
    Task.await_many(tasks, :infinity)
    elapsed = System.monotonic_time() - started
    verify.()
    cleanup.()
    seconds = System.convert_time_unit(elapsed, :native, :microsecond) / 1_000_000
    workers * iterations / seconds
  end

  defp prepare(:ets, key, expected) do
    table = :ets.new(:baseline_counter, [:public])
    default = {key, 0, 0, 0, 0, 0, 0}
    :ets.insert(table, default)
    increment = fn -> :ets.update_counter(table, key, {2, 1}, default); {:ok, key} end
    verify = fn -> [{^key, ^expected, _, _, _, _, _}] = :ets.lookup(table, key) end
    {increment, verify, fn -> :ets.delete(table) end}
  end

  defp prepare(:sharded, key, expected) do
    table = :ets.new(:sharded_counter, [:public, read_concurrency: true])
    ref = :counters.new(6, [:write_concurrency])
    :ets.insert(table, {key, ref})
    increment = fn -> [{^key, r}] = :ets.lookup(table, key); :counters.add(r, 1, 1); {:ok, key} end
    verify = fn -> ^expected = :counters.get(ref, 1) end
    {increment, verify, fn -> :ets.delete(table) end}
  end

  defp prepare(:hybrid, key, expected) do
    {:ok, ^key} = Counters.create(key)
    increment = fn -> {:ok, ^key} = Counters.increment(key) end
    verify = fn -> {:ok, ^expected} = Counters.get_inserts(key) end
    {increment, verify, fn -> Counters.delete(key) end}
  end

  defp repeat(_increment, 0), do: :ok
  defp repeat(increment, n), do: (increment.(); repeat(increment, n - 1))

  defp wait(ref, index, target) do
    if :atomics.get(ref, index) < target, do: (:erlang.yield(); wait(ref, index, target))
  end
end

Logflare.Bench.IngestCounterThroughput.run()
