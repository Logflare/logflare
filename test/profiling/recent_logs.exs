alias Logflare.Sources
import Logflare.Factory

Mimic.copy(Broadway)

Mimic.stub(Broadway, :push_messages, fn _, _ -> :ok end)

source =
  Sources.get(:"f74e843a-e09d-42e1-b2bc-1915e75b53c5")

# |> Sources.refresh_source_metrics_for_ingest()
# |> Sources.preload_defaults()

# # v1
# Logflare.Source.RecentLogsServer.start_link([source: source])

batch = for _ <- 1..100, do: Logflare.Factory.build(:log_event, message: "some message")

for _ <- 1..50 do
  # v1
  # Logflare.Source.RecentLogsServer.push(source, batch)

  # v2
  Logflare.Backends.push_recent_events(source, batch)
end

# 2024-06-24 v1
# CNT    ACC (ms)    OWN (ms)
# 226,540     376.652     370.102

# Current: 2024-06-24 v2
# CNT        ACC (ms)    OWN (ms)
# 233,266     394.510     384.937
