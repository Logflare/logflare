# Run unchanged on baseline and candidate revisions:
#   MIX_ENV=test ../bin/x mix run --no-start bench/log_event_cleaner.exs
# LF_BENCH_FILTER selects scenario names with a regular expression.
# LF_BENCH_OUTPUT optionally writes throughput/allocation results as JSON.

alias Logflare.Logs.IngestTransformers

benchmark_time = String.to_integer(System.get_env("LF_BENCH_TIME") || "5")
warmup_time = String.to_integer(System.get_env("LF_BENCH_WARMUP") || "2")
memory_time = String.to_integer(System.get_env("LF_BENCH_MEMORY_TIME") || "3")

map_scenarios =
  for size <- [1, 4, 8, 16, 32, 33, 64], into: %{} do
    clean = Map.new(1..size, &{"field_#{&1}", &1})

    {size,
     %{
       "map: clean #{size}" => clean,
       "map: sparse dirty #{size}" => Map.put(clean, "field_1", nil),
       "map: half empty #{size}" =>
         Map.new(1..size, &{"field_#{&1}", if(rem(&1, 2) == 0, do: nil, else: &1)}),
       "map: mostly dirty #{size}" =>
         Map.new(1..size, &{"field-#{&1}", if(rem(&1, 4) == 0, do: &1, else: nil)}),
       "map: all empty #{size}" => Map.new(1..size, &{"field_#{&1}", nil}),
       "map: all unsafe #{size}" => Map.new(1..size, &{"field-#{&1}", &1})
     }}
  end
  |> Map.values()
  |> Enum.reduce(%{}, &Map.merge/2)

depth_scenarios =
  for depth <- [8, 32, 64, 128, 256, 512], list? <- [false, true], into: %{} do
    input =
      Enum.reduce(1..depth, %{"keep" => 1, "drop" => nil}, fn _, inner ->
        if list?, do: %{"nested" => [inner]}, else: %{"nested" => inner}
      end)

    kind = if list?, do: "map/list", else: "map"
    {"depth: dirty #{kind} #{depth}", input}
  end

clean = Map.new(1..64, &{"field_#{&1}", &1})
list_clean = %{"items" => Enum.map(1..64, &%{"id" => &1, "value" => "ok"})}

scenarios =
  map_scenarios
  |> Map.merge(depth_scenarios)
  |> Map.merge(%{
    "map: sparse dirty 66" => clean |> Map.put("drop", nil) |> Map.put("bad-key", true),
    "map: tiny mixed dirty 4" => %{"keep" => 1, "drop" => nil, "nested" => [nil], "bad-key" => 2},
    "map: collision 66" => Map.merge(clean, %{"a!b" => 1, "a?b" => 2}),
    "list: clean 64 maps" => list_clean,
    "list: one nested change" => put_in(list_clean, ["items", Access.at(32), "drop"], nil),
    "list: half empty" => %{"items" => Enum.flat_map(1..32, &[nil, %{"id" => &1}])}
  })

scenarios =
  case System.get_env("LF_BENCH_FILTER") do
    nil ->
      scenarios

    filter ->
      Map.filter(scenarios, fn {name, _} -> Regex.match?(Regex.compile!(filter), name) end)
  end

if map_size(scenarios) == 0, do: raise("LF_BENCH_FILTER matched no scenarios")

suite =
  Benchee.run(
    %{
      "cleaner" => fn payload ->
        IngestTransformers.transform(payload, :clean_to_bigquery_column_spec)
      end
    },
    inputs: scenarios,
    time: benchmark_time,
    warmup: warmup_time,
    memory_time: memory_time,
    parallel: 1,
    print: [benchmarking: true, configuration: true, fast_warning: false]
  )

if output = System.get_env("LF_BENCH_OUTPUT") do
  results =
    Enum.map(suite.scenarios, fn scenario ->
      %{
        input: scenario.input_name,
        ips: scenario.run_time_data.statistics.ips,
        memory_bytes: scenario.memory_usage_data.statistics.average
      }
    end)

  File.write!(output, Jason.encode!(results, pretty: true))
end
