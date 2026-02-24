alias Logflare.Sources
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

key1 = {source.id, nil, make_ref()}
key2 = {source.id, nil, make_ref()}

onek =
  for _ <- 1..1_000 do
    Logflare.Factory.build(:log_event)
  end

tenk =
  for _ <- 1..10_000 do
    Logflare.Factory.build(:log_event)
  end

# hundredk =
#   for _ <- 1..100_000 do
#     Logflare.Factory.build(:log_event)
#   end

require Ex2ms

ms =
  Ex2ms.fun do
    {event_id, _status, _event} -> true
  end

Benchee.run(
  %{
    "destroy and recreate" => fn _ ->
      :ets.delete(IngestEventQueue.get_tid(key2))
      IngestEventQueue.upsert_tid(key2)
    end,
    "delete_all_objects" => fn _ ->
      :ets.delete_all_objects(IngestEventQueue.get_tid(key2))
    end,
    "select_delete" => fn _ ->
      :ets.select_delete(IngestEventQueue.get_tid(key2), ms)
    end,
    "truncate pending 100" => fn _ ->
      IngestEventQueue.truncate_table(key2, :pending, 100)
    end,
    "truncate pending 0" => fn _ ->
      IngestEventQueue.truncate_table(key2, :pending, 0)
    end,
    "truncate all 100" => fn _ ->
      IngestEventQueue.truncate_table(key2, :all, 100)
    end,
    "truncate all 0" => fn _ ->
      IngestEventQueue.truncate_table(key2, :all, 0)
    end,
    "truncate ingested 100" => fn _ ->
      IngestEventQueue.truncate_table(key2, :ingested, 100)
    end,
    "truncate ingested 0" => fn _ ->
      IngestEventQueue.truncate_table(key2, :ingested, 0)
    end
  },
  before_each: fn input ->
    # IngestEventQueue.upsert_tid(key1)
    IngestEventQueue.upsert_tid(key2)

    # IngestEventQueue.add_to_table(key1, input)
    IngestEventQueue.add_to_table(key2, input)
    {_pending, ingested} = Enum.split(input, round(length(input) / 2))
    IngestEventQueue.mark_ingested(key2, ingested)

    input
  end,
  inputs: %{
    "1k" => onek,
    "10k" => tenk
    # "100k" => hundredk,
    # "v2" => v2_source
  },
  time: 4,
  memory_time: 0
)

# 2024-12-16 baseline
# ##### With input 10k #####
# Name                            ips        average  deviation         median         99th %
# truncate ingested 0          957.25        1.04 ms    ±11.45%        1.01 ms        1.48 ms
# truncate all 0               814.22        1.23 ms    ±11.73%        1.17 ms        1.73 ms
# destroy and recreate         789.53        1.27 ms    ±11.10%        1.24 ms        1.66 ms
# delete_all_objects           750.82        1.33 ms    ±12.73%        1.33 ms        1.90 ms
# select_delete                647.51        1.54 ms    ±11.29%        1.48 ms        2.21 ms
# truncate ingested 100        427.72        2.34 ms    ±12.09%        2.27 ms        3.32 ms
# truncate pending 100         383.53        2.61 ms     ±7.92%        2.58 ms        3.28 ms
# truncate pending 0           372.31        2.69 ms     ±8.99%        2.66 ms        3.51 ms
# truncate all 100             250.31        4.00 ms    ±15.86%        3.83 ms        5.21 ms

# Comparison:
# truncate ingested 0          957.25
# truncate all 0               814.22 - 1.18x slower +0.184 ms
# destroy and recreate         789.53 - 1.21x slower +0.22 ms
# delete_all_objects           750.82 - 1.27x slower +0.29 ms
# select_delete                647.51 - 1.48x slower +0.50 ms
# truncate ingested 100        427.72 - 2.24x slower +1.29 ms
# truncate pending 100         383.53 - 2.50x slower +1.56 ms
# truncate pending 0           372.31 - 2.57x slower +1.64 ms
# truncate all 100             250.31 - 3.82x slower +2.95 ms

# ##### With input 1k #####
# Name                            ips        average  deviation         median         99th %
# delete_all_objects          16.79 K       59.56 μs    ±21.70%       55.21 μs      123.99 μs
# truncate ingested 0         15.31 K       65.30 μs    ±14.51%       62.25 μs      102.75 μs
# truncate all 0              15.26 K       65.54 μs    ±18.66%       61.67 μs      123.27 μs
# destroy and recreate        13.77 K       72.60 μs    ±30.84%       72.38 μs      136.25 μs
# select_delete               11.27 K       88.75 μs    ±13.58%       85.46 μs      147.53 μs
# truncate pending 100         5.65 K      176.93 μs    ±10.37%      172.05 μs      247.83 μs
# truncate ingested 100        5.64 K      177.32 μs     ±9.51%      172.92 μs      238.22 μs
# truncate pending 0           4.98 K      200.96 μs     ±9.38%      197.13 μs      280.21 μs
# truncate all 100             3.70 K      270.23 μs     ±9.08%      263.38 μs      376.23 μs

# Comparison:
# delete_all_objects          16.79 K
# truncate ingested 0         15.31 K - 1.10x slower +5.74 μs
# truncate all 0              15.26 K - 1.10x slower +5.98 μs
# destroy and recreate        13.77 K - 1.22x slower +13.04 μs
# select_delete               11.27 K - 1.49x slower +29.19 μs
# truncate pending 100         5.65 K - 2.97x slower +117.36 μs
# truncate ingested 100        5.64 K - 2.98x slower +117.75 μs
# truncate pending 0           4.98 K - 3.37x slower +141.40 μs
# truncate all 100             3.70 K - 4.54x slower +210.66 μs

# 2024-12-16: after using select_delete and insert
# ##### With input 10k #####
# Name                            ips        average  deviation         median         99th %
# truncate all 0               789.76        1.27 ms    ±10.88%        1.23 ms        1.64 ms
# destroy and recreate         737.32        1.36 ms    ±10.71%        1.32 ms        1.75 ms
# delete_all_objects           724.07        1.38 ms    ±28.37%        1.32 ms        2.08 ms
# truncate ingested 0          677.53        1.48 ms    ±11.47%        1.43 ms        1.95 ms
# truncate pending 0           621.05        1.61 ms     ±7.08%        1.58 ms        1.92 ms
# select_delete                605.67        1.65 ms    ±10.60%        1.59 ms        2.14 ms
# truncate ingested 100        587.34        1.70 ms     ±9.20%        1.66 ms        2.17 ms
# truncate pending 100         552.29        1.81 ms     ±7.26%        1.78 ms        2.20 ms
# truncate all 100             493.30        2.03 ms     ±9.65%        1.95 ms        2.58 ms

# Comparison:
# truncate all 0               789.76
# destroy and recreate         737.32 - 1.07x slower +0.0901 ms
# delete_all_objects           724.07 - 1.09x slower +0.115 ms
# truncate ingested 0          677.53 - 1.17x slower +0.21 ms
# truncate pending 0           621.05 - 1.27x slower +0.34 ms
# select_delete                605.67 - 1.30x slower +0.38 ms
# truncate ingested 100        587.34 - 1.34x slower +0.44 ms
# truncate pending 100         552.29 - 1.43x slower +0.54 ms
# truncate all 100             493.30 - 1.60x slower +0.76 ms

# ##### With input 1k #####
# Name                            ips        average  deviation         median         99th %
# truncate all 0              15.79 K       63.35 μs    ±16.85%       60.38 μs      116.20 μs
# delete_all_objects          14.06 K       71.12 μs    ±22.67%       69.13 μs      139.57 μs
# destroy and recreate        12.81 K       78.04 μs    ±25.68%       76.33 μs      147.74 μs
# select_delete               11.15 K       89.67 μs    ±14.57%       86.25 μs      155.82 μs
# truncate ingested 0         10.36 K       96.50 μs     ±9.07%       94.08 μs      127.98 μs
# truncate pending 0          10.01 K       99.91 μs    ±12.49%       96.13 μs      164.25 μs
# truncate all 100             2.77 K      360.40 μs    ±14.55%      338.04 μs      582.32 μs
# truncate pending 100         2.76 K      362.31 μs    ±13.17%      342.58 μs      564.19 μs
# truncate ingested 100        2.74 K      364.55 μs    ±14.32%      342.17 μs      558.54 μs

# Comparison:
# truncate all 0              15.79 K
# delete_all_objects          14.06 K - 1.12x slower +7.77 μs
# destroy and recreate        12.81 K - 1.23x slower +14.70 μs
# select_delete               11.15 K - 1.42x slower +26.32 μs
# truncate ingested 0         10.36 K - 1.52x slower +33.15 μs
# truncate pending 0          10.01 K - 1.58x slower +36.56 μs
# truncate all 100             2.77 K - 5.69x slower +297.05 μs
# truncate pending 100         2.76 K - 5.72x slower +298.96 μs
# truncate ingested 100        2.74 K - 5.75x slower +301.20 μs
