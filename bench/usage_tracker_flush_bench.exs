# Usage: mix run bench/usage_tracker_flush_bench.exs
#   KV_FLUSH_N=500000      # entries in the frozen buffer
#   KV_FLUSH_CHUNK=5000    # chunk size for the streamed approach
#
# Compares draining the frozen usage buffer two ways:
#   A) :ets.tab2list/1  -> one big list on the heap, grouped once
#   B) :ets.match/3     -> streamed in chunks, grouped per chunk
# Reports the term memory held at once (the reviewer's concern) and traversal time.
# The DB write is stubbed out so we measure only the ETS-read strategy.

n = System.get_env("KV_FLUSH_N", "500000") |> String.to_integer()
chunk_size = System.get_env("KV_FLUSH_CHUNK", "5000") |> String.to_integer()

table = :ets.new(:flush_bench_buf, [:set, :public])
objects = for i <- 1..n, do: {{1, "key_#{i}"}}
:ets.insert(table, objects)

words_to_mb = fn words -> Float.round(words * :erlang.system_info(:wordsize) / 1_048_576, 2) end

# group exactly like KeyValues.bump_usages/2 does, minus the DB round trip
group = fn pairs -> Enum.group_by(pairs, fn {uid, _} -> uid end, fn {_, k} -> k end) end

drain_whole = fn ->
  pairs = table |> :ets.tab2list() |> Enum.map(fn {pair} -> pair end)
  _ = group.(pairs)
  :ok
end

drain_chunked = fn ->
  step = fn step, acc ->
    case acc do
      :"$end_of_table" ->
        :ok

      {rows, cont} ->
        pairs = Enum.map(rows, fn [pair] -> pair end)
        _ = group.(pairs)
        step.(step, :ets.match(cont))
    end
  end

  step.(step, :ets.match(table, {:"$1"}, chunk_size))
end

# --- Memory held at once -----------------------------------------------------
whole_list = table |> :ets.tab2list() |> Enum.map(fn {pair} -> pair end)
{one_chunk, _cont} = :ets.match(table, {:"$1"}, chunk_size)
one_chunk_pairs = Enum.map(one_chunk, fn [pair] -> pair end)

IO.puts("\n=== buffer: #{n} entries, chunk size: #{chunk_size} ===")
IO.puts("ETS table memory:        #{words_to_mb.(:ets.info(table, :memory))} MB")

IO.puts(
  "tab2list whole list:     #{words_to_mb.(:erts_debug.size(whole_list))} MB held on heap at once"
)

IO.puts(
  "match/3 single chunk:    #{words_to_mb.(:erts_debug.size(one_chunk_pairs))} MB held on heap at once"
)

IO.puts(
  "memory ratio (whole/chunk): #{Float.round(:erts_debug.size(whole_list) / max(:erts_debug.size(one_chunk_pairs), 1), 1)}x\n"
)

# keep terms reachable until after printing, then drop
_ = {whole_list, one_chunk_pairs}

# --- Traversal time ----------------------------------------------------------
Benchee.run(
  %{
    "tab2list (whole)" => drain_whole,
    "match/3 (chunked, #{chunk_size})" => drain_chunked
  },
  time: 3,
  warmup: 1,
  memory_time: 2
)
