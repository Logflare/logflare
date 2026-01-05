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
    "new (optimized)" => fn events ->
      IngestEventQueue.add_to_table({source_id, backend_id}, events)
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
#
# 2026-01-01 - Benchmarking commit 98e61fa47 changes
#
##### With input 1000 events #####
# Name                      ips        average  deviation         median         99th %
# new (optimized)        4.56 K      219.23 μs     ±6.63%      218.25 μs      261.69 μs
# old (legacy)           4.54 K      220.07 μs     ±7.28%      219.25 μs      253.52 μs

# Comparison:
# new (optimized)        4.56 K
# old (legacy)           4.54 K - 1.00x slower +0.84 μs

# Memory usage statistics:

# Name               Memory usage
# new (optimized)       124.12 KB
# old (legacy)          135.20 KB - 1.09x memory usage +11.08 KB

# **All measurements for memory usage were the same**

# Reduction count statistics:

# Name            Reduction count
# new (optimized)          6.81 K
# old (legacy)             7.19 K - 1.06x reduction count +0.39 K

# **All measurements for reduction count were the same**

# ##### With input 250 events #####
# Name                      ips        average  deviation         median         99th %
# old (legacy)          16.45 K       60.79 μs     ±5.50%       60.54 μs       71.33 μs
# new (optimized)       16.15 K       61.91 μs    ±40.33%       61.33 μs       73.67 μs

# Comparison:
# old (legacy)          16.45 K
# new (optimized)       16.15 K - 1.02x slower +1.11 μs

# Memory usage statistics:

# Name               Memory usage
# old (legacy)           53.98 KB
# new (optimized)        49.50 KB - 0.92x memory usage -4.48438 KB

# **All measurements for memory usage were the same**

# Reduction count statistics:

# Name            Reduction count
# old (legacy)             3.89 K
# new (optimized)          3.36 K - 0.86x reduction count -0.53300 K

# ------------------------------------------------------------------------------------------------
# 2026-01-05 - bugfixes
# ------------------------------------------------------------------------------------------------
# ##### With input 1000 events #####
# Name                      ips        average  deviation         median         99th %
# new (optimized)        3.11 K      321.58 μs     ±6.50%      318.75 μs      376.42 μs

# Memory usage statistics:

# Name               Memory usage
# new (optimized)        99.35 KB

# **All measurements for memory usage were the same**

# Reduction count statistics:

# Name            Reduction count
# new (optimized)          5.72 K
