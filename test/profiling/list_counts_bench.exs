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
    "list_counts" => fn sid_bid ->
      IngestEventQueue.list_counts(sid_bid)
    end,
    "list_counts_with_tids" => fn sid_bid ->
      IngestEventQueue.list_counts_with_tids(sid_bid)
    end,
    "list_queues_with_tids" => fn sid_bid ->
      IngestEventQueue.list_queues_with_tids(sid_bid)
    end,
    "list_queues" => fn sid_bid ->
      IngestEventQueue.list_queues(sid_bid)
    end,
    # "new" => fn sid_bid ->
    #   IngestEventQueue.list_counts(sid_bid, legacy: false)
    # end
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
# ##### With input 25 sources, 3 backends, 30 queues each #####
# Name                            ips        average  deviation         median         99th %
# list_queues                 17.46 K       57.29 μs    ±10.44%       56.63 μs       68.17 μs
# list_queues_with_tids       17.26 K       57.94 μs     ±9.71%       57.21 μs          70 μs
# list_counts_with_tids       16.91 K       59.13 μs    ±12.89%       58.29 μs       72.14 μs
# list_counts                 16.84 K       59.39 μs    ±20.62%       58.33 μs       74.88 μs

# Comparison:
# list_queues                 17.46 K
# list_queues_with_tids       17.26 K - 1.01x slower +0.65 μs
# list_counts_with_tids       16.91 K - 1.03x slower +1.84 μs
# list_counts                 16.84 K - 1.04x slower +2.10 μs

# Memory usage statistics:

# Name                     Memory usage
# list_queues                   1.70 KB
# list_queues_with_tids         2.10 KB - 1.23x memory usage +0.40 KB
# list_counts_with_tids         3.57 KB - 2.10x memory usage +1.87 KB
# list_counts                   3.98 KB - 2.34x memory usage +2.28 KB

# **All measurements for memory usage were the same**

# Reduction count statistics:

# Name                  Reduction count
# list_queues                    8.31 K
# list_queues_with_tids          8.35 K - 1.00x reduction count +0.0330 K
# list_counts_with_tids          8.49 K - 1.02x reduction count +0.178 K
# list_counts                    8.48 K - 1.02x reduction count +0.164 K
