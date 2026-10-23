# Normal-VM microbenchmark of the original source-counter API versus the
# current implementation. Only the ETS write-concurrency option differs.
defmodule Logflare.Bench.BaselineSourceCounter do
  @moduledoc false
  @table :baseline_counter

  def start, do: :ets.new(@table, [:public, :named_table])
  def create(key), do: (:ets.update_counter(@table, key, {2, 0}, default(key)); {:ok, key})
  def increment(key), do: (:ets.update_counter(@table, key, {2, 1}, default(key)); {:ok, key})
  def get_inserts(key), do: {:ok, elem(hd(:ets.lookup(@table, key)), 1)}
  def delete(key), do: :ets.delete(@table, key)
  defp default(key), do: {key, 0, 0, 0, 0, 0, 0}
end

defmodule Logflare.Bench.IngestCounterThroughput do
  @moduledoc false

  alias Logflare.Sources.Counters
  alias Logflare.Bench.BaselineSourceCounter

  def run do
    BaselineSourceCounter.start()
    {:ok, _pid} = Counters.start_link()
    IO.puts("OTP=#{:erlang.system_info(:otp_release)} schedulers=#{System.schedulers_online()}")

    for workers <- [1, 2, 8], mode <- [:ets, :auto] do
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
    {:ok, ^key} = BaselineSourceCounter.create(key)
    increment = fn -> {:ok, ^key} = BaselineSourceCounter.increment(key) end
    verify = fn -> {:ok, ^expected} = BaselineSourceCounter.get_inserts(key) end
    {increment, verify, fn -> BaselineSourceCounter.delete(key) end}
  end

  defp prepare(:auto, key, expected) do
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
