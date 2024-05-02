defmodule Logflare.Backends.BufferProducerTest do
  use Logflare.DataCase, async: false

  alias Logflare.PubSubRates
  alias Logflare.Backends.BufferProducer

  test "BufferProducer broadcasts every n ms" do
    PubSubRates.subscribe(:buffers)

    pid =
      start_supervised!({BufferProducer, broadcast_interval: 100, source_token: :"some-token"})

    :timer.sleep(300)
    assert_receive {:buffers, :"some-token", _payload}
    send(pid, {:add_to_buffer, [:something]})
    :timer.sleep(300)
    assert PubSubRates.Cache.get_cluster_buffers(:"some-token") == 1
  end

  test "BufferProducer does not broadcast if prolonged zero" do
    PubSubRates.subscribe(:buffers)

    pid =
      start_supervised!({BufferProducer, broadcast_interval: 100, source_token: :"some-token"})

    :timer.sleep(1_000)
    {:messages, messages} = :erlang.process_info(self(), :messages)
    len = length(messages)
    assert len < 5
    send(pid, {:add_to_buffer, [:something]})
    :timer.sleep(300)
    {:messages, messages} = :erlang.process_info(self(), :messages)
    new_len = length(messages)
    assert new_len != len
    assert PubSubRates.Cache.get_cluster_buffers(:"some-token") == 1
  end

  test "BufferProducer broadcasts every n seconds with backend differentiation" do
    PubSubRates.subscribe(:buffers)

    start_supervised!(
      {BufferProducer,
       broadcast_interval: 100, backend_token: "some-backend", source_token: :"some-token"}
    )

    :timer.sleep(200)
    assert_receive {:buffers, :"some-token", "some-backend", _payload}

    assert PubSubRates.Cache.get_cluster_buffers(:"some-token", "some-backend") == 0
  end
end
