alias Logflare.Sources

Mimic.copy(Broadway)

Mimic.stub(Broadway, :push_messages, fn _, _ -> :ok end)

ver = System.argv() |> Enum.at(0)

source =
  Sources.get(:"f74e843a-e09d-42e1-b2bc-1915e75b53c5")
  |> Sources.refresh_source_metrics_for_ingest()
  |> Sources.preload_defaults()
  |> case do
    s when ver == "v1" -> %{s | v2_pipeline: false}
    s -> %{s | v2_pipeline: true}
  end

:ok = Logflare.Sources.Source.Supervisor.ensure_started(source)

batch = for _ <- 1..1000, do: %{message: "some message"}

if ver == "v1" do
  Logflare.Logs.ingest_logs(batch, source)
else
  Logflare.Backends.ingest_logs(batch, source)
end

# Current: 2024-06-02 v1
# CNT    ACC (ms)    OWN (ms)
# 2,085,049    3441.462    3439.073

# Current: 2024-06-02 v2
# CNT        ACC (ms)    OWN (ms)
# 1,706,387    2816.090    2810.488

# Current: 2024-07-05 v2
# CNT        ACC (ms)    OWN (ms)
# 1,717,432    2831.382    2827.665
