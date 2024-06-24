alias Logflare.Sources

Mimic.copy(Broadway)

Mimic.stub(Broadway, :push_messages, fn _, _ -> :ok end)

arg1 = System.argv() |> Enum.at(0)
source = Sources.get(:"f74e843a-e09d-42e1-b2bc-1915e75b53c5")
Logflare.Backends.BufferProducer.start_link(backend_token: nil, source_token: source.token)

:timer.sleep(60_000)

# Current: 2024-06-02 bef
# CNT    ACC (ms)    OWN (ms)
# 37976   60007.640     195.062
