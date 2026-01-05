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
##### With input 1000 events #####
# Name                      ips        average  deviation         median         99th %
# new (optimized)        4.53 K      220.96 μs     ±6.22%      220.50 μs      259.67 μs

# Memory usage statistics:

# Name               Memory usage
# new (optimized)       135.13 KB

# **All measurements for memory usage were the same**

# Reduction count statistics:

# Name            Reduction count
# new (optimized)          7.19 K
