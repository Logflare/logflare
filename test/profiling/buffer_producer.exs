alias Logflare.Sources
import Logflare.Factory
Mimic.copy(Broadway)

Mimic.stub(Broadway, :push_messages, fn _, _ -> :ok end)

arg1 = System.argv() |> Enum.at(0)
source = Sources.get(:"f74e843a-e09d-42e1-b2bc-1915e75b53c5")
{:ok, pid} = Logflare.Backends.BufferProducer.start_link(backend_token: nil, source_token: source.token, source: source)

:timer.sleep(100)
batch = for _ <- 1..1000 do
  %{id: "123", message: :something}
end
for _ <- 1..600 do
  # send(pid, {:add_to_buffer, batch})
  :ok = Logflare.Backends.IngestEvents.add_to_table(source, batch)
end
Logflare.Backends.IngestEvents.get_table_info(source) |> dbg()
:timer.sleep(5_000)

# 2024-06-26 v1.7.5
# CNT    ACC (ms)    OWN (ms)
# 3,050,283    5013.375    4886.998

# GenStage.Buffer.queue_last/7                                    600600    4557.226    2302.478
# :queue.drop/1                                                   550000    1101.073     550.254
# :queue.in/2                                                     600000     602.346     601.142
# GenStage.Buffer.pop_and_increment_wheel/1                       550000     550.626     550.314
# :lists.split/2                                                      22     550.576       0.044
# :lists.split/3                                                  550000     550.532     550.263

# 2024-06-26 ets poc
# CNT    ACC (ms)    OWN (ms)
# 1,213,364    6324.526    1822.835

# Enum."-reduce/3-lists^foldl/2-0-"/3                             600676    1803.046    1201.367
# anonymous fn/2 in Logflare.Backends.IngestEvents.add_to_tabl    600000     600.604     600.302

# no compression
# Logflare.Backends.IngestEvents.get_table_info(source) #=> [
#   id: #Reference<0.2155957719.3514171396.156576>,
#   decentralized_counters: false,
#   read_concurrency: true,
#   write_concurrency: true,
#   compressed: false,
#   memory: 26409519,  = 26MB without compression
#   owner: #PID<0.1139.0>,
#   heir: :none,
#   name: :source_ingest_events,
#   size: 1,200,000,
#   node: :nonode@nohost,
#   named_table: false,
#   type: :duplicate_bag,
#   keypos: 1,
#   protection: :public
# ]
