alias Logflare.Sources

Mimic.copy(Broadway)

Mimic.stub(Broadway, :push_messages, fn _, _ -> :ok end)

arg1 = System.argv() |> Enum.at(0)
source = Sources.get(:"f74e843a-e09d-42e1-b2bc-1915e75b53c5")

:timer.sleep(100)

# add 1k batches of 100
batch =
  for _ <- 1..100 do
    %Logflare.LogEvent{id: "123", message: :something}
  end

Logflare.Backends.IngestEventQueue.upsert_tid({source.id, nil})

for _ <- 1..1000 do
  :ok = Logflare.Backends.IngestEventQueue.add_to_table({source, nil}, batch)
end

# 2024-07-05 after
# CNT    ACC (ms)    OWN (ms)
# 214,375     434.933     325.246
