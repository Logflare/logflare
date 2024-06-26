alias Logflare.Sources
import Logflare.Factory
Mimic.copy(Broadway)

Mimic.stub(Broadway, :push_messages, fn _, _ -> :ok end)

arg1 = System.argv() |> Enum.at(0)
source = Sources.get(:"f74e843a-e09d-42e1-b2bc-1915e75b53c5")
{:ok, pid} = Logflare.Backends.BufferProducer.start_link(backend_token: nil, source_token: source.token)

batch = for _ <- 1..1000 do
  :something
end
for _ <- 1..600 do
  send(pid, {:add_to_buffer, batch})
end
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
