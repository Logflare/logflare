alias Logflare.Backends.IngestEventQueue

# Create a test user and source
user = Logflare.Factory.insert(:user)
source = Logflare.Factory.insert(:source, user: user)
source_id = source.id
backend_id = nil

# Create multiple queues to simulate production load
queue_count = 30

queues =
  for _ <- 1..queue_count do
    # Spawn a process to act as the queue owner (simulating a backend worker)
    pid = spawn(fn -> :timer.sleep(:infinity) end)
    key = {source_id, backend_id, pid}
    IngestEventQueue.upsert_tid(key)
    key
  end

queues = [{source.id, nil, nil} | queues]

# Generate test events with different batch sizes
inputs = %{
  # "250 events" => for(_ <- 1..250, do: Logflare.Factory.build(:log_event)),
  "1000 events" => for(_ <- 1..1_000, do: Logflare.Factory.build(:log_event))
}

Benchee.run(
  %{
    # "add_to_table" => fn events ->
    #   IngestEventQueue.add_to_table({source_id, backend_id}, events)
    # end,
    "add_to_table - 50 chunk size" => fn events ->
      IngestEventQueue.add_to_table({source_id, backend_id}, events, chunk_size: 50)
    end,
    "add_to_table - 100 chunk size" => fn events ->
      IngestEventQueue.add_to_table({source_id, backend_id}, events, chunk_size: 100)
    end,
    "add_to_table - 250 chunk size" => fn events ->
      IngestEventQueue.add_to_table({source_id, backend_id}, events, chunk_size: 250)
    end,
    "add_to_table - 500 chunk size" => fn events ->
      IngestEventQueue.add_to_table({source_id, backend_id}, events, chunk_size: 500)
    end,
    "add_to_table - 1000 chunk size" => fn events ->
      IngestEventQueue.add_to_table({source_id, backend_id}, events, chunk_size: 1000)
    end
    # "old (legacy)" => fn events ->
    #   IngestEventQueue.add_to_table({source_id, backend_id}, events, legacy: true)
    # end
  },
  inputs: inputs,
  before_each: fn events ->
    # Clear queues before each iteration to reset state
    for key <- queues do
      IngestEventQueue.truncate_table(key, :all, 0)
    end

    events
  end,
  time: 5,
  warmup: 2,
  memory_time: 3,
  reduction_time: 3
)

# Historical results:
# Run with: mix run test/profiling/add_to_table_bench.exs
# ##### With input 1000 events #####
# Name                   ips        average  deviation         median         99th %
# add_to_table        4.26 K      234.86 μs    ±12.01%      231.88 μs      305.10 μs

# Memory usage statistics:

# Name            Memory usage
# add_to_table       135.13 KB

# **All measurements for memory usage were the same**

# Reduction count statistics:

# Name         Reduction count
# add_to_table          7.19 K

## 2026-01-05  Increase Chunk Size ##
# ##### With input 1000 events #####
# Name                                     ips        average  deviation         median         99th %
# add_to_table - 100 chunk size         4.80 K      208.52 μs     ±6.77%      208.13 μs      255.90 μs
# add_to_table - 50 chunk size          4.69 K      213.11 μs     ±7.91%      212.71 μs      244.80 μs
# add_to_table - 250 chunk size         4.52 K      221.04 μs     ±7.38%      219.17 μs      265.13 μs
# add_to_table - 1000 chunk size        4.45 K      224.51 μs    ±10.00%      224.08 μs      261.65 μs
# add_to_table - 500 chunk size         4.13 K      242.20 μs     ±6.15%      241.21 μs      283.29 μs

# Comparison:
# add_to_table - 100 chunk size         4.80 K
# add_to_table - 50 chunk size          4.69 K - 1.02x slower +4.59 μs
# add_to_table - 250 chunk size         4.52 K - 1.06x slower +12.52 μs
# add_to_table - 1000 chunk size        4.45 K - 1.08x slower +15.99 μs
# add_to_table - 500 chunk size         4.13 K - 1.16x slower +33.68 μs

# Memory usage statistics:

# Name                              Memory usage
# add_to_table - 100 chunk size        128.26 KB
# add_to_table - 50 chunk size         135.13 KB - 1.05x memory usage +6.88 KB
# add_to_table - 250 chunk size        124.13 KB - 0.97x memory usage -4.12500 KB
# add_to_table - 1000 chunk size       122.05 KB - 0.95x memory usage -6.20313 KB
# add_to_table - 500 chunk size        122.76 KB - 0.96x memory usage -5.50000 KB

# **All measurements for memory usage were the same**

# Reduction count statistics:

# Name                           Reduction count
# add_to_table - 100 chunk size           6.98 K
# add_to_table - 50 chunk size            7.20 K - 1.03x reduction count +0.22 K
# add_to_table - 250 chunk size           6.88 K - 0.99x reduction count -0.09900 K
# add_to_table - 1000 chunk size          7.13 K - 1.02x reduction count +0.151 K
# add_to_table - 500 chunk size           8.37 K - 1.20x reduction count +1.39 K
