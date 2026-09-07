defmodule IngestGenerationEtsBenchmark do
  @moduledoc false

  def run do
    workers = env_integer("LF_BENCH_WORKERS", System.schedulers_online())
    mutations_per_worker = env_integer("LF_BENCH_MUTATIONS_PER_WORKER", 50_000)
    select_calls = env_integer("LF_BENCH_SELECT_CALLS", 20_000)
    table_rows = env_integer("LF_BENCH_TABLE_ROWS", 100_000)
    select_limit = env_integer("LF_BENCH_SELECT_LIMIT", 100)
    samples = env_integer("LF_BENCH_SAMPLES", 5)

    IO.puts("OTP #{System.otp_release()}, schedulers=#{System.schedulers_online()}")

    IO.puts(
      "generation mutations: workers=#{workers}, insert/delete pairs per worker=#{mutations_per_worker}, samples=#{samples}"
    )

    mutation_configs = [
      {"baseline decentralized_counters=false", false},
      {"candidate decentralized_counters=true", true}
    ]

    Enum.each(mutation_configs, fn {_name, decentralized?} ->
      mutation_sample(decentralized?, workers, div(mutations_per_worker, 10))
    end)

    mutation_samples =
      for sample <- 1..samples,
          {name, decentralized?} <- sample_order(mutation_configs, sample) do
        {name, mutation_sample(decentralized?, workers, mutations_per_worker)}
      end

    mutation_results =
      for {name, _decentralized?} <- mutation_configs do
        results = for {^name, result} <- mutation_samples, do: result
        report_mutations(name, results, workers * mutations_per_worker * 2)
        {name, results}
      end

    IO.puts(
      "generation reads: rows=#{table_rows}, select limit=#{select_limit}, calls per sample=#{select_calls}, samples=#{samples}"
    )

    select_results = selection_samples(table_rows, select_limit, select_calls, samples)

    report_comparison(mutation_results, select_results)
  end

  defp mutation_sample(decentralized?, workers, pairs_per_worker) do
    tid =
      :ets.new(:generation_benchmark, [
        :public,
        :set,
        {:decentralized_counters, decentralized?},
        {:write_concurrency, :auto},
        {:read_concurrency, true}
      ])

    barrier = :atomics.new(2, signed: false)

    tasks =
      for worker <- 1..workers do
        Task.async(fn ->
          :atomics.add_get(barrier, 1, 1)
          await_counter(barrier, 2, 1)

          for sequence <- 1..pairs_per_worker do
            key = {worker, sequence}
            true = :ets.insert(tid, {key, sequence})
            true = :ets.delete(tid, key)
          end
        end)
      end

    await_counter(barrier, 1, workers)
    started_at = System.monotonic_time()
    :atomics.put(barrier, 2, 1)
    Task.await_many(tasks, :infinity)
    elapsed = System.monotonic_time() - started_at

    result = %{
      seconds: System.convert_time_unit(elapsed, :native, :microsecond) / 1_000_000,
      memory_words: :ets.info(tid, :memory),
      final_size: :ets.info(tid, :size)
    }

    :ets.delete(tid)
    result
  end

  defp selection_samples(table_rows, select_limit, select_calls, samples) do
    tid =
      :ets.new(:generation_select_benchmark, [
        :public,
        :set,
        {:decentralized_counters, true},
        {:write_concurrency, :auto},
        {:read_concurrency, true}
      ])

    rows = for id <- 1..table_rows, do: {id, id}
    true = :ets.insert(tid, rows)
    match_spec = [{{:_, :"$1"}, [], [:"$1"]}]

    with_size = fn ->
      size = :ets.info(tid, :size)
      :ets.select(tid, match_spec, min(select_limit, max(size, 1)))
    end

    direct = fn -> :ets.select(tid, match_spec, select_limit) end

    operations = [
      {"baseline info(:size) + select", with_size},
      {"candidate direct select", direct}
    ]

    Enum.each(operations, fn {_name, operation} ->
      for _ <- 1..100, do: operation.()
    end)

    samples_by_name =
      for sample <- 1..samples,
          {name, operation} <- sample_order(operations, sample) do
        {microseconds, checksum} =
          :timer.tc(fn ->
            for _ <- 1..select_calls, reduce: 0 do
              acc -> acc + selected_count(operation.())
            end
          end)

        if checksum != select_calls * select_limit do
          raise "unexpected selection checksum: #{checksum}"
        end

        {name, microseconds / 1_000_000}
      end

    results =
      for {name, _operation} <- operations, into: %{} do
        durations = for {^name, duration} <- samples_by_name, do: duration
        report_rates(name, durations, select_calls)
        {name, durations}
      end

    :ets.delete(tid)
    results
  end

  defp selected_count({events, _continuation}), do: length(events)
  defp selected_count(:"$end_of_table"), do: 0

  defp await_counter(counter, index, target) do
    if :atomics.get(counter, index) < target do
      :erlang.yield()
      await_counter(counter, index, target)
    end
  end

  defp sample_order(configurations, sample) when rem(sample, 2) == 0,
    do: Enum.reverse(configurations)

  defp sample_order(configurations, _sample), do: configurations

  defp report_mutations(name, results, mutations_per_sample) do
    durations = Enum.map(results, & &1.seconds)
    memory_words = results |> Enum.map(& &1.memory_words) |> Enum.max()
    final_sizes = results |> Enum.map(& &1.final_size) |> Enum.uniq()

    IO.puts(
      "  #{name}: #{format_rate(mutations_per_sample / median(durations))} mutations/s " <>
        "(median #{format_seconds(median(durations))}, range #{format_seconds(Enum.min(durations))}-#{format_seconds(Enum.max(durations))}, " <>
        "max memory #{memory_words} words, final sizes #{inspect(final_sizes)})"
    )
  end

  defp report_rates(name, durations, operations_per_sample) do
    IO.puts(
      "  #{name}: #{format_rate(operations_per_sample / median(durations))} calls/s " <>
        "(median #{format_seconds(median(durations))}, range #{format_seconds(Enum.min(durations))}-#{format_seconds(Enum.max(durations))})"
    )
  end

  defp report_comparison(mutation_results, select_results) do
    [{_, baseline_mutations}, {_, candidate_mutations}] = mutation_results

    baseline_mutation_time = baseline_mutations |> Enum.map(& &1.seconds) |> median()
    candidate_mutation_time = candidate_mutations |> Enum.map(& &1.seconds) |> median()

    baseline_select_time = median(select_results["baseline info(:size) + select"])
    candidate_select_time = median(select_results["candidate direct select"])

    IO.puts("comparison (positive means candidate is faster):")

    IO.puts(
      "  decentralized generation mutations: #{format_percent(speedup(baseline_mutation_time, candidate_mutation_time))}"
    )

    IO.puts(
      "  direct generation selection: #{format_percent(speedup(baseline_select_time, candidate_select_time))}"
    )
  end

  defp speedup(baseline, candidate), do: (baseline / candidate - 1) * 100

  defp median(values) do
    sorted = Enum.sort(values)
    count = length(sorted)
    middle = div(count, 2)

    if rem(count, 2) == 1 do
      Enum.at(sorted, middle)
    else
      (Enum.at(sorted, middle - 1) + Enum.at(sorted, middle)) / 2
    end
  end

  defp env_integer(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end

  defp format_rate(value), do: :erlang.float_to_binary(value / 1, decimals: 0)
  defp format_seconds(value), do: :erlang.float_to_binary(value / 1, decimals: 4) <> "s"
  defp format_percent(value), do: :erlang.float_to_binary(value / 1, decimals: 1) <> "%"
end

IngestGenerationEtsBenchmark.run()
