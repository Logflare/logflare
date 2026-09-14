# Run from the repository root with:
#   MIX_ENV=dev ../bin/x mix run --no-start bench/log_event_cleaner.exs
#
# Compare this command unchanged on the benchmark-only baseline commit and its
# copy-on-write cleaner child.

alias Logflare.Logs.IngestTransformers

benchmark_time = String.to_integer(System.get_env("LF_BENCH_TIME") || "5")
warmup_time = String.to_integer(System.get_env("LF_BENCH_WARMUP") || "2")
memory_time = String.to_integer(System.get_env("LF_BENCH_MEMORY_TIME") || "3")

clean = Map.new(1..64, &{"field_#{&1}", &1})
sparse_dirty = clean |> Map.put("drop", nil) |> Map.put("bad-key", true)
dense_empty = Map.new(1..64, &{"field_#{&1}", nil})
dense_unsafe = Map.new(1..64, &{"field-#{&1}", &1})
half_empty = Map.new(1..64, &{"field_#{&1}", if(rem(&1, 2) == 0, do: nil, else: &1)})

list_clean = %{"items" => Enum.map(1..64, &%{"id" => &1, "value" => "ok"})}
list_sparse_dirty = put_in(list_clean, ["items", Access.at(32), "drop"], nil)
list_half_empty = %{"items" => Enum.flat_map(1..32, &[nil, %{"id" => &1}])}

scenarios = %{
  "map: clean 64" => clean,
  "map: sparse dirty 66" => sparse_dirty,
  "map: half empty 64" => half_empty,
  "map: all empty 64" => dense_empty,
  "map: all unsafe 64" => dense_unsafe,
  "list: clean 64 maps" => list_clean,
  "list: one nested change" => list_sparse_dirty,
  "list: half empty" => list_half_empty
}

Benchee.run(
  Map.new(scenarios, fn {name, payload} ->
    {name, fn -> IngestTransformers.transform(payload, :clean_to_bigquery_column_spec) end}
  end),
  time: benchmark_time,
  warmup: warmup_time,
  memory_time: memory_time,
  parallel: 1,
  print: [benchmarking: true, configuration: true, fast_warning: false]
)
