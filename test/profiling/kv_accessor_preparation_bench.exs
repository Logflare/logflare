# Run with MIX_ENV=test mix run --no-start test/profiling/kv_accessor_preparation_bench.exs
# Repeat with BENCH_REVERSE=1 to reverse raw/prepared order.
defmodule Logflare.KeyValues.AccessorPreparationBenchmark do
  alias Logflare.KeyValues
  alias Logflare.KeyValues.Cache

  @keys 10_000
  @repeats 5
  @extractions 200_000

  @spec run() :: :ok
  def run do
    for app <- [:cachex, :warpath], do: {:ok, _} = Application.ensure_all_started(app)
    {:ok, _} = Cachex.start_link(Cache, compressed: true)
    nil = Process.whereis(Logflare.Repo)

    IO.write("""
    Elixir #{System.version()}, OTP #{:erlang.system_info(:otp_release)}
    #{@keys} keys, compressed Cachex, #{@repeats} repeats; median batch µs/op
    Preparation, seeding, deletion and verification are outside timing; no database
    ETS allocation excludes shared binary backing, process heaps and prepared paths
    """)

    keys = Enum.map(1..@keys, &"project_#{&1}")

    cases = [
      {"flat", "org_id"},
      {"nested", "nested.id"},
      {"jsonpath", "$.nested.id"},
      {"filter", "$.routes[?(@.enabled == true)].name"}
    ]

    cases = if System.get_env("BENCH_REVERSE") == "1", do: Enum.reverse(cases), else: cases

    Enum.each(cases, &run_case(&1, keys))
  end

  @spec run_case({String.t(), String.t()}, [String.t()]) :: :ok
  defp run_case({label, raw}, keys) do
    prepared = KeyValues.prepare_accessor(raw)
    value = fixture(1)
    expected = KeyValues.extract_value(value, raw)
    ^expected = KeyValues.extract_value(value, prepared)
    {:ok, _} = Cachex.clear(Cache)

    for {key, index} <- Enum.with_index(keys, 1) do
      {:ok, true} = Cachex.put(Cache, {:lookup, [42, key, nil]}, {:cached, fixture(index)})
    end

    modes = [{"raw", raw}, {"prepared", prepared}]
    modes = if System.get_env("BENCH_REVERSE") == "1", do: Enum.reverse(modes), else: modes

    Enum.each(modes, fn {mode, accessor} -> run_mode({label, mode}, raw, accessor, keys) end)
  end

  @spec run_mode({String.t(), String.t()}, String.t(), term(), [String.t()]) :: :ok
  defp run_mode({label, mode}, raw, accessor, keys) do
    value = fixture(1)
    extract = fn -> KeyValues.extract_value(value, accessor) end
    repeat(2_000, extract)
    extraction_us = measure(fn -> repeat(@extractions, extract) end, @extractions)

    verify = fn ->
      Enum.each(Enum.with_index(keys, 1), fn {key, index} ->
        expected = KeyValues.extract_value(fixture(index), raw)
        ^expected = Cache.lookup(42, key, accessor)
        {:cached, ^expected} = Cachex.get!(Cache, {:lookup, [42, key, raw]})
      end)
    end

    verify.()

    lookup_batch = fn -> Enum.each(keys, &Cache.lookup(42, &1, accessor)) end

    reset = fn ->
      Enum.each(keys, &Cachex.del!(Cache, {:lookup, [42, &1, raw]}))
      @keys = Cachex.size!(Cache)
    end

    fill_us = measure(lookup_batch, @keys, reset)
    verify.()
    hit_us = measure(fn -> repeat(10, lookup_batch) end, @keys * 10)
    entries = Cachex.size!(Cache)
    true = entries == @keys * 2
    memory_mb = Cachex.inspect!(Cache, {:memory, :bytes}) / 1_000_000

    IO.write(
      "#{label}/#{mode}: extraction=#{format(extraction_us)}, " <>
        "fill=#{format(fill_us)}, hit=#{format(hit_us)}, " <>
        "entries=#{entries}, ETS_MB=#{format(memory_mb)}\n"
    )
  end

  @spec fixture(pos_integer()) :: map()
  defp fixture(index) do
    %{
      "org_id" => "org_#{index}",
      "name" => "Project #{index}",
      "nested" => %{"id" => index},
      "org" => %{"region" => "us-east-1", "plan" => "team"},
      "routes" => [
        %{"name" => "primary", "enabled" => true},
        %{"name" => "backup", "enabled" => false}
      ]
    }
  end

  @spec measure((-> term()), pos_integer(), (-> term())) :: float()
  defp measure(batch, count, before_batch \\ fn -> :ok end) do
    for _ <- 1..@repeats do
      before_batch.()

      Task.async(fn ->
        :erlang.garbage_collect()
        started = System.monotonic_time()
        batch.()
        elapsed = System.monotonic_time() - started
        System.convert_time_unit(elapsed, :native, :nanosecond) / count / 1_000
      end)
      |> Task.await(:infinity)
    end
    |> Statistex.median()
  end

  @spec repeat(non_neg_integer(), (-> term())) :: :ok
  defp repeat(0, _operation), do: :ok

  defp repeat(count, operation) do
    operation.()
    repeat(count - 1, operation)
  end

  @spec format(float()) :: String.t()
  defp format(value), do: :erlang.float_to_binary(value, decimals: 3)
end

Logflare.KeyValues.AccessorPreparationBenchmark.run()
