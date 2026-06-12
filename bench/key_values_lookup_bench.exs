# Usage: mix run bench/key_values_lookup_bench.exs
#   KV_BENCH_N=500000  # entries pre-filled into the cache and usage buffer
#
# Measures Cache.lookup/3 cache-hit throughput and UsageTracker.touch/2 latency,
# both against an empty buffer/cache and against a large pre-filled one, to confirm
# the touch overhead stays ~O(1) as the ETS buffer grows.

alias Logflare.KeyValues.Cache
alias Logflare.KeyValues.UsageTracker

# `mix run` boots the full application, so Cache and UsageTracker are already
# started by Logflare.ContextCache.Supervisor.

n = System.get_env("KV_BENCH_N", "500000") |> String.to_integer()
user_id = 1
hot_key = "bench-hot-key"
value = %{"org_id" => "org_abc"}
buffers = {:key_value_usage_buffer_0, :key_value_usage_buffer_1}

# The periodic 30s flush would drain the buffer (and write to the DB) mid-run.
# Restart the tracker with a long interval so the pre-fill survives the benchmark.
Application.put_env(:logflare, UsageTracker, flush_interval: :timer.hours(24))

wait_up = fn self ->
  case Process.whereis(UsageTracker) do
    nil -> Process.sleep(10) && self.(self)
    pid -> pid
  end
end

if pid = Process.whereis(UsageTracker) do
  ref = Process.monitor(pid)
  GenServer.stop(UsageTracker)

  receive do
    {:DOWN, ^ref, _, _, _} -> :ok
  after
    2_000 -> :ok
  end
end

wait_up.(wait_up)

active_buffer = fn ->
  ref = :persistent_term.get({UsageTracker, :active_idx_ref})
  elem(buffers, :atomics.get(ref, 1))
end

run = fn label ->
  buffer = active_buffer.()
  {:ok, csize} = Cachex.size(Cache)
  IO.puts("\n========== #{label} ==========")
  IO.puts("usage buffer size: #{:ets.info(buffer, :size)}")
  IO.puts("cache size:        #{csize}")

  Benchee.run(
    %{
      "lookup hit (+touch)" => fn -> Cache.lookup(user_id, hot_key) end,
      "touch (overwrite existing key)" => fn -> UsageTracker.touch(user_id, hot_key) end
    },
    time: 2,
    warmup: 1
  )
end

# --- Baseline: empty buffer, tiny cache --------------------------------------
:ets.delete_all_objects(active_buffer.())
Cachex.clear(Cache)
Cachex.put(Cache, {:lookup, [user_id, hot_key, nil]}, {:cached, value})

run.("EMPTY buffer + tiny cache")

# --- Fill the cache and the usage buffer with N distinct entries -------------
1..n
|> Stream.map(fn i -> {{:lookup, [user_id, "key_#{i}", nil]}, {:cached, value}} end)
|> Stream.chunk_every(25_000)
|> Enum.each(&Cachex.put_many(Cache, &1))

# Bulk-insert N distinct keys straight into the active buffer (same shape touch
# writes). We overwrite an existing hot key in the benchmark rather than inserting
# new keys, so the table stays size N instead of ballooning during measurement;
# for an ETS :set the insert cost is independent of key presence anyway.
objects = for i <- 1..n, do: {{user_id, "key_#{i}"}}
:ets.insert(active_buffer.(), objects)

run.("FILLED buffer + cache (#{n} entries)")
