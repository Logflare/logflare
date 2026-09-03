defmodule Logflare.Bench.IngestCounterLcnt do
  @moduledoc false

  @default_iterations 100_000

  def run do
    ensure_lock_counting!()

    scenario = System.get_env("SCENARIO", "source")
    workers = env_integer("WORKERS", System.schedulers_online())
    iterations = env_integer("ITERATIONS", @default_iterations)
    {increment, verify} = prepare(scenario, workers * iterations)
    barrier = :atomics.new(2, signed: false)

    tasks =
      for _worker <- 1..workers do
        Task.async(fn ->
          :atomics.add_get(barrier, 1, 1)
          await_counter(barrier, 2, 1)

          repeat(increment, iterations)
        end)
      end

    await_counter(barrier, 1, workers)
    :ok = :lcnt.rt_mask([:db])
    :ok = :lcnt.clear()
    started_at = System.monotonic_time()
    :atomics.put(barrier, 2, 1)
    Task.await_many(tasks, :infinity)
    elapsed = System.monotonic_time() - started_at
    :ok = :lcnt.collect()
    :ok = verify.()

    seconds = System.convert_time_unit(elapsed, :native, :microsecond) / 1_000_000
    operations = workers * iterations

    IO.puts(
      "scenario=#{scenario} workers=#{workers} iterations=#{iterations} " <>
        "seconds=#{format_seconds(seconds)} operations_per_second=#{round(operations / seconds)}"
    )

    :lcnt.conflicts(
      combine: false,
      max_locks: 20,
      sort: :time,
      thresholds: [colls: 0],
      print: [:name, :id, :type, :tries, :colls, :ratio, :time, :duration]
    )
  end

  defp prepare("source", expected) do
    {:ok, _pid} = Logflare.Sources.Counters.start_link()
    {:ok, :lcnt_profile_source} = Logflare.Sources.Counters.create(:lcnt_profile_source)

    increment = fn ->
      {:ok, :lcnt_profile_source} =
        Logflare.Sources.Counters.increment(:lcnt_profile_source)
    end

    verify = fn ->
      {:ok, ^expected} = Logflare.Sources.Counters.get_inserts(:lcnt_profile_source)
      :ok
    end

    {increment, verify}
  end

  defp prepare("system", expected) do
    {:ok, :total_logs_logged} =
      Logflare.SystemMetrics.AllLogsLogged.create(:total_logs_logged, 0)

    increment = fn ->
      {:ok, :total_logs_logged} =
        Logflare.SystemMetrics.AllLogsLogged.increment(:total_logs_logged)
    end

    verify = fn ->
      {:ok, ^expected} =
        Logflare.SystemMetrics.AllLogsLogged.log_count(:total_logs_logged)

      :ok
    end

    {increment, verify}
  end

  defp prepare(scenario, _expected) do
    raise "unknown SCENARIO=#{inspect(scenario)}; expected source or system"
  end

  defp ensure_lock_counting! do
    if :erlang.system_info(:build_type) != :lcnt do
      raise "start an OTP lock-counting VM with -emu_type lcnt"
    end
  end

  defp repeat(_increment, 0), do: :ok

  defp repeat(increment, remaining) do
    increment.()
    repeat(increment, remaining - 1)
  end

  defp await_counter(counter, index, target) do
    if :atomics.get(counter, index) < target do
      :erlang.yield()
      await_counter(counter, index, target)
    end
  end

  defp env_integer(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end

  defp format_seconds(value), do: :erlang.float_to_binary(value / 1, decimals: 6)
end

Logflare.Bench.IngestCounterLcnt.run()
