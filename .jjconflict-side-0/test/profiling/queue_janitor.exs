alias Logflare.Sources
alias Logflare.Backends.IngestEventQueue
alias Logflare.Backends.IngestEventQueue.QueueJanitor
import Logflare.Factory
# arg1 = System.argv() |> Enum.at(0)
source = insert(:source, user: insert(:user))

le = build(:log_event, message: "some value")
IngestEventQueue.upsert_tid({source.id, nil})
:ok = IngestEventQueue.add_to_table({source, nil}, [le])
QueueJanitor.start_link(source: source, backend: nil)
:timer.sleep(5_000)
# Logflare.Backends.IngestEventQueue.upsert_tid({source, nil})
# for _ <- 1..1_000 do
#   le = build(:log_event, message: "some value")
#   :ok = IngestEventQueue.add_to_table({source, nil}, [le])
#   :timer.sleep(5)
# end

# 2024-07-11 current
# CNT    ACC (ms)    OWN (ms)
# 7727    5022.971      13.010
