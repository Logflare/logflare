alias Logflare.Sources
alias Logflare.Users
require Phoenix.ConnTest
Mimic.copy(Broadway)
Mimic.copy(Logflare.Backends)
Mimic.copy(Logflare.Logs)
Mimic.copy(Logflare.Partners)
alias Logflare.Backends.IngestEventQueue

Mimic.stub(Logflare.Backends, :ingest_logs, fn _, _ -> :ok end)
Mimic.stub(Logflare.Logs, :ingest_logs, fn _, _ -> :ok end)
# Mimic.stub(Broadway, :push_messages, fn _, _ -> :ok end)
ver = System.argv() |> Enum.at(0)

source = Sources.get(:"9f37d86e-e4fa-4ef2-a47e-e8d4ac1fceba")

# v2_source = Sources.get(:"94d07aab-30f5-460e-8871-eb85f4674e35")

# user = Users.get(v1_source.user_id)

Benchee.run(
  %{
    "list_pending_counts" => fn _input ->
      IngestEventQueue.list_pending_counts({source.id, nil})
    end,
    "cached_local_pending_buffer_len" => fn _input ->
      Logflare.Backends.cached_local_pending_buffer_len(source.id)
    end,
    "get_buffers" => fn _input ->
      Logflare.PubSubRates.Cache.get_buffers(source.id, nil)
    end,
    "cache_local_buffer_lens" => fn _ ->
      Logflare.Backends.cache_local_buffer_lens(source.id)
    end
  },
  before_scenario: fn prev ->
    key1 = {source.id, nil, make_ref()}
    key2 = {source.id, nil, make_ref()}
    IngestEventQueue.upsert_tid(key1)
    IngestEventQueue.upsert_tid(key2)

    events =
      for _ <- 1..10_000 do
        Logflare.Factory.build(:log_event)
      end

    IngestEventQueue.add_to_table(key1, events)
    IngestEventQueue.add_to_table(key2, events)

    prev
  end,
  inputs: %{
    "v1" => source
    # "v2" => v2_source
  },
  time: 4,
  memory_time: 0
)

# 2024-12-14 addition of cache_local_buffer_lens/1
# ##### With input v1 #####
# Name                                      ips        average  deviation         median         99th %
# get_buffers                         2247.46 K        0.44 μs  ±5589.04%        0.38 μs        0.58 μs
# cached_local_pending_buffer_len      264.75 K        3.78 μs   ±429.42%        3.50 μs        5.42 μs
# cache_local_buffer_lens              148.17 K        6.75 μs   ±184.04%        6.25 μs       12.21 μs
# list_pending_counts                    1.07 K      933.85 μs    ±22.27%      887.48 μs     1523.25 μs

# Comparison:
# get_buffers                         2247.46 K
# cached_local_pending_buffer_len      264.75 K - 8.49x slower +3.33 μs
# cache_local_buffer_lens              148.17 K - 15.17x slower +6.30 μs
# list_pending_counts                    1.07 K - 2098.80x slower +933.41 μs
