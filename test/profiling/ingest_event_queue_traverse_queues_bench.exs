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

  all_queues = []

  all_queues =
    for source <- sources,
        backend <- backends,
        reduce: all_queues do
      acc ->
        source_id = source.id
        backend_id = backend.id

        # Create startup queue
        startup_key = {source_id, backend_id, nil}
        IngestEventQueue.upsert_tid(startup_key)

        # Create worker queues
        worker_queues =
          for _ <- 1..queues_per_combo do
            pid = spawn(fn -> :timer.sleep(:infinity) end)
            key = {source_id, backend_id, pid}
            IngestEventQueue.upsert_tid(key)
            key
          end

        # Add some events to each queue
        events = for(_ <- 1..100, do: Logflare.Factory.build(:log_event))

        for key <- [startup_key | worker_queues] do
          IngestEventQueue.add_to_table(key, events)
        end

        [{source_id, backend_id} | acc]
    end

  all_queues
end

# Define inputs with different queue distributions
inputs = %{
  "10 sources, 3 backends, 10 queues each" => {10, 3, 10}
}

# Simple function to use with traverse_queues
traverse_func = fn objs, acc ->
  items =
    for {key, _tid} <- objs do
      key
    end

  items ++ acc
end

Benchee.run(
  %{
    "traverse_queues" => fn sid_bid ->
      IngestEventQueue.traverse_queues(sid_bid, traverse_func, [])
    end,
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
# ##### With input 10 sources, 3 backends, 10 queues each #####
# Name                      ips        average  deviation         median         99th %
# traverse_queues      170.18 K        5.88 μs   ±102.66%        5.71 μs        7.46 μs

# Memory usage statistics:

# Name               Memory usage
# traverse_queues         1.50 KB

# **All measurements for memory usage were the same**

# Reduction count statistics:

# Name            Reduction count
# traverse_queues             389
