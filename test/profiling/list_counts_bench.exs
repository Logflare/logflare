alias Logflare.Backends.IngestEventQueue

# Create a test user
user = Logflare.Factory.insert(:user)

# Helper function to set up queues for a given scenario
setup_queues = fn sources_count, backends_count, queues_per_combo ->
  sources =
    for _ <- 1..sources_count do
      Logflare.Factory.insert(:source, user: user)
    end

  backends =
    for _ <- 1..backends_count do
      Logflare.Factory.insert(:backend)
    end

  # First, collect all queue operations without inserting
  queue_operations =
    for source <- sources,
        backend <- backends do
      source_id = source.id
      backend_id = backend.id

      # Prepare startup queue operation
      startup_key = {source_id, backend_id, nil}

      # Prepare worker queue operations
      worker_operations =
        for _ <- 1..queues_per_combo do
          pid = spawn(fn -> :timer.sleep(:infinity) end)
          key = {source_id, backend_id, pid}
          key
        end

      {source_id, backend_id, [startup_key | worker_operations]}
    end

  # Shuffle the queue operations
  shuffled_operations = Enum.shuffle(queue_operations)

  # Now insert queues in shuffled order
  all_queues =
    for {source_id, backend_id, keys} <- shuffled_operations do
      # Insert each queue
      for key <- keys do
        IngestEventQueue.upsert_tid(key)
      end

      # Add some events to each queue
      events = for(_ <- 1..100, do: Logflare.Factory.build(:log_event))

      for key <- keys do
        IngestEventQueue.add_to_table(key, events)
      end

      {source_id, backend_id}
    end

  all_queues
end

# Define inputs with different queue distributions
inputs = %{
  "25 sources, 3 backends, 30 queues each" => {25, 3, 30}
}

Benchee.run(
  %{
    "legacy" => fn sid_bid ->
      IngestEventQueue.list_counts(sid_bid, legacy: true)
    end,
    "new" => fn sid_bid ->
      IngestEventQueue.list_counts(sid_bid, legacy: false)
    end
  },
  inputs: inputs,
  before_scenario: fn {sources_count, backends_count, queues_per_combo} = input ->
    # Clean up all existing mappings
    IngestEventQueue.delete_all_mappings()

    # Set up queues for this scenario
    queues = setup_queues.(sources_count, backends_count, queues_per_combo)

    # Return the first queue key to benchmark
    {input, List.first(queues)}
  end,
  before_each: fn {_input, sid_bid} ->
    # Return the queue key for this iteration
    sid_bid
  end,
  time: 5,
  warmup: 2,
  memory_time: 3,
  reduction_time: 3
)

# Historical results:
# Run with: mix run test/profiling/list_counts_bench.exs

# 1st jan 2026

# ##### With input 25 sources, 3 backends, 30 queues each #####
# Name             ips        average  deviation         median         99th %
# new           3.25 M        0.31 μs  ±6820.99%        0.29 μs        0.42 μs
# legacy      0.0152 M       65.72 μs     ±8.57%       64.54 μs          76 μs

# Comparison:
# new           3.25 M
# legacy      0.0152 M - 213.40x slower +65.41 μs

# Memory usage statistics:

# Name      Memory usage
# new           0.102 KB
# legacy        22.01 KB - 216.69x memory usage +21.91 KB

# **All measurements for memory usage were the same**

# Reduction count statistics:

# Name   Reduction count
# new           0.0140 K
# legacy          8.75 K - 624.93x reduction count +8.73 K
