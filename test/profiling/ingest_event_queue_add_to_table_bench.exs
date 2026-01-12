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

startup_queue = {source.id, nil, nil}
IngestEventQueue.upsert_tid(startup_queue)
queues = [{source.id, nil, nil} | queues]

# Generate test events with different batch sizes
inputs = %{
  "250 events" => for(_ <- 1..250, do: Logflare.Factory.build(:log_event)),
  "1000 events" => for(_ <- 1..1_000, do: Logflare.Factory.build(:log_event))
}

random_queue = Enum.random(queues)
random_tid = IngestEventQueue.get_tid(random_queue)

Benchee.run(
  %{
    # "add_to_table" => fn events ->
    #   IngestEventQueue.add_to_table({source_id, backend_id}, events)
    # end,
    # "add_to_table - 50 chunk size" => fn events ->
    #   IngestEventQueue.add_to_table({source_id, backend_id}, events, chunk_size: 50)
    # end,
    # "add_to_table - 100 chunk size, no_get_tid=false, check_queue_size=false" => fn events ->
    #   IngestEventQueue.add_to_table({source_id, backend_id}, events,
    #     chunk_size: 100,
    #     no_get_tid: false,
    #     check_queue_size: false
    #   )
    # end,
    "add_to_table - 100 chunk, no_get_tid=true, check_queue_size=true, bag" => fn events ->
      IngestEventQueue.add_to_table({source_id, backend_id}, events,
        chunk_size: 100,
        no_get_tid: true,
        check_queue_size: true
      )
    end,
    "direct insert baseline, insert individual" => fn [event | _] = events ->
      :ets.insert(random_tid, {event.id, :pending, event})
    end,
    "direct insert baseline, insert list" => fn [event | _] = events ->
      :ets.insert(random_tid, {event.id, :pending, events})
    end,
    "direct insert baseline, insert batch" => fn events ->
      objects =
        for %{id: id} = event <- events do
          {id, :pending, event}
        end

      :ets.insert(random_tid, objects)
    end,
    "direct insert baseline, insert batch in for loop" => fn events ->
      for %{id: id} = event <- events do
        :ets.insert(random_tid, {id, :pending, event})
      end
    end
    # "add_to_table - 100 chunk, no_get_tid=true, check_queue_size=false" => fn events ->
    #   IngestEventQueue.add_to_table({source_id, backend_id}, events,
    #     chunk_size: 100,
    #     no_get_tid: true,
    #     check_queue_size: false
    #   )
    # end
    # "add_to_table - 250 chunk size" => fn events ->
    #   IngestEventQueue.add_to_table({source_id, backend_id}, events, chunk_size: 250)
    # end,
    # "add_to_table - 500 chunk size" => fn events ->
    #   IngestEventQueue.add_to_table({source_id, backend_id}, events, chunk_size: 500)
    # end,
    # "add_to_table - 1000 chunk size" => fn events ->
    #   IngestEventQueue.add_to_table({source_id, backend_id}, events, chunk_size: 1000)
    # end
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

###### 2026-01-05  Add no_get_tid and check_queue_size options #####

# ##### With input 1000 events #####
# Name                                                                              ips        average  deviation         median         99th %
# add_to_table - 100 chunk, no_get_tid=true, check_queue_size=false              4.78 K      209.39 μs     ±5.25%      209.63 μs      241.49 μs
# add_to_table - 100 chunk, no_get_tid=true, check_queue_size=true               4.73 K      211.55 μs     ±6.46%      211.38 μs      245.17 μs
# add_to_table - 100 chunk size, no_get_tid=false, check_queue_size=false        4.71 K      212.38 μs     ±7.33%      212.04 μs      254.47 μs

# Comparison:
# add_to_table - 100 chunk, no_get_tid=true, check_queue_size=false              4.78 K
# add_to_table - 100 chunk, no_get_tid=true, check_queue_size=true               4.73 K - 1.01x slower +2.16 μs
# add_to_table - 100 chunk size, no_get_tid=false, check_queue_size=false        4.71 K - 1.01x slower +2.99 μs

# Memory usage statistics:

# Name                                                                       Memory usage
# add_to_table - 100 chunk, no_get_tid=true, check_queue_size=false             100.19 KB
# add_to_table - 100 chunk, no_get_tid=true, check_queue_size=true              100.19 KB - 1.00x memory usage +0 KB
# add_to_table - 100 chunk size, no_get_tid=false, check_queue_size=false       128.29 KB - 1.28x memory usage +28.10 KB

# **All measurements for memory usage were the same**

# Reduction count statistics:

# Name                                                                    Reduction count
# add_to_table - 100 chunk, no_get_tid=true, check_queue_size=false                5.78 K
# add_to_table - 100 chunk, no_get_tid=true, check_queue_size=true                 5.78 K - 1.00x reduction count +0 K
# add_to_table - 100 chunk size, no_get_tid=false, check_queue_size=false          6.99 K - 1.21x reduction count +1.21 K

###### 2026-01-06  Direct Insert Baseline #####

# ##### With input 1000 events #####
# Name                                                                            ips        average  deviation         median         99th %
# direct insert baseline, insert individual                                 3023.16 K        0.33 μs    ±65.46%        0.29 μs        0.75 μs
# direct insert baseline, insert list                                          7.17 K      139.56 μs     ±6.18%      139.41 μs      164.14 μs
# add_to_table - 100 chunk, no_get_tid=true, check_queue_size=true, bag        4.33 K      231.15 μs    ±10.28%      228.71 μs      291.11 μs
# direct insert baseline, insert batch                                         4.27 K      233.99 μs     ±8.45%      232.13 μs      286.39 μs
# direct insert baseline, insert batch in for loop                             3.98 K      250.99 μs     ±7.10%      249.63 μs      294.77 μs

# Comparison:
# direct insert baseline, insert individual                                 3023.16 K
# direct insert baseline, insert list                                          7.17 K - 421.92x slower +139.23 μs
# add_to_table - 100 chunk, no_get_tid=true, check_queue_size=true, bag        4.33 K - 698.80x slower +230.82 μs
# direct insert baseline, insert batch                                         4.27 K - 707.38x slower +233.66 μs
# direct insert baseline, insert batch in for loop                             3.98 K - 758.78x slower +250.66 μs

# Memory usage statistics:

# Name                                                                     Memory usage
# direct insert baseline, insert individual                                   0.0313 KB
# direct insert baseline, insert list                                         0.0313 KB - 1.00x memory usage +0 KB
# add_to_table - 100 chunk, no_get_tid=true, check_queue_size=true, bag       107.34 KB - 3435.00x memory usage +107.31 KB
# direct insert baseline, insert batch                                         62.50 KB - 2000.00x memory usage +62.47 KB
# direct insert baseline, insert batch in for loop                             62.53 KB - 2001.00x memory usage +62.50 KB

# **All measurements for memory usage were the same**

# Reduction count statistics:

# Name                                                                  Reduction count
# direct insert baseline, insert individual                                   0.00200 K
# direct insert baseline, insert list                                         0.00200 K - 1.00x reduction count +0 K
# add_to_table - 100 chunk, no_get_tid=true, check_queue_size=true, bag          6.14 K - 3071.00x reduction count +6.14 K
# direct insert baseline, insert batch                                           3.40 K - 1699.50x reduction count +3.40 K
# direct insert baseline, insert batch in for loop                               4.02 K - 2011.00x reduction count +4.02 K

# **All measurements for reduction count were the same**

# ##### With input 250 events #####
# Name                                                                            ips        average  deviation         median         99th %
# direct insert baseline, insert individual                                 3200.32 K        0.31 μs    ±60.91%        0.29 μs        0.71 μs
# direct insert baseline, insert list                                         28.73 K       34.80 μs     ±9.51%       34.54 μs       43.12 μs
# direct insert baseline, insert batch                                        18.29 K       54.67 μs    ±12.34%       54.08 μs       71.28 μs
# direct insert baseline, insert batch in for loop                            17.08 K       58.56 μs    ±15.92%       57.38 μs       73.80 μs
# add_to_table - 100 chunk, no_get_tid=true, check_queue_size=true, bag       16.58 K       60.32 μs    ±23.19%       59.25 μs       77.06 μs

# Comparison:
# direct insert baseline, insert individual                                 3200.32 K
# direct insert baseline, insert list                                         28.73 K - 111.38x slower +34.49 μs
# direct insert baseline, insert batch                                        18.29 K - 174.95x slower +54.35 μs
# direct insert baseline, insert batch in for loop                            17.08 K - 187.41x slower +58.25 μs
# add_to_table - 100 chunk, no_get_tid=true, check_queue_size=true, bag       16.58 K - 193.05x slower +60.01 μs

# Memory usage statistics:

# Name                                                                     Memory usage
# direct insert baseline, insert individual                                   0.0313 KB
# direct insert baseline, insert list                                         0.0313 KB - 1.00x memory usage +0 KB
# direct insert baseline, insert batch                                         15.63 KB - 500.00x memory usage +15.59 KB
# direct insert baseline, insert batch in for loop                             15.66 KB - 501.00x memory usage +15.63 KB
# add_to_table - 100 chunk, no_get_tid=true, check_queue_size=true, bag        30.05 KB - 961.75x memory usage +30.02 KB

# **All measurements for memory usage were the same**

# Reduction count statistics:

# Name                                                                  Reduction count
# direct insert baseline, insert individual                                   0.00200 K
# direct insert baseline, insert list                                         0.00200 K - 1.00x reduction count +0 K
# direct insert baseline, insert batch                                           4.20 K - 2102.00x reduction count +4.20 K
# direct insert baseline, insert batch in for loop                               1.67 K - 832.50x reduction count +1.66 K
# add_to_table - 100 chunk, no_get_tid=true, check_queue_size=true, bag          3.23 K - 1617.00x reduction count +3.23 K
