alias Logflare.Sources

Mimic.copy(Broadway)

Mimic.stub(Broadway, :push_messages, fn _, _ -> :ok end)

arg1 = System.argv() |> Enum.at(0)
source = Sources.get(:"f74e843a-e09d-42e1-b2bc-1915e75b53c5")
# Logflare.Backends.BufferProducer.start_link(backend: nil, source: source)

:timer.sleep(100)

# add 1k batches of 100
batch =
  for _ <- 1..100 do
    %Logflare.LogEvent{id: "123", message: :something}
  end

Logflare.Backends.IngestEventQueue.upsert_tid({source, nil})

for _ <- 1..1000 do
  # send(pid, {:add_to_buffer, batch})
  :ok = Logflare.Backends.IngestEventQueue.add_to_table({source, nil}, batch)
end

# Logflare.Backends.IngestEventQueue.get_table_info({source, bac})
# :timer.sleep(5_000)

# Current: 2024-06-02 bef
# CNT    ACC (ms)    OWN (ms)
# 37976   60007.640     195.062

# 2024-07-05 after
# 214,337
