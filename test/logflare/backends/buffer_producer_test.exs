defmodule Logflare.Backends.BufferProducerTest do
  use Logflare.DataCase, async: false

  alias Logflare.PubSubRates
  alias Logflare.Backends.BufferProducer

  import ExUnit.CaptureLog

  test "BufferProducer broadcasts every n ms" do
    PubSubRates.subscribe(:buffers)
    user = insert(:user)
    %{token: source_token} = source = insert(:source, user: user)

    pid =
      start_supervised!({BufferProducer, broadcast_interval: 100, source_token: source.token})

    :timer.sleep(300)
    assert_receive {:buffers, ^source_token, _payload}
    send(pid, {:add_to_buffer, [:something]})
    :timer.sleep(300)
    assert PubSubRates.Cache.get_cluster_buffers(source_token) == 1
  end

  test "BufferProducer broadcasts every n seconds with backend differentiation" do
    PubSubRates.subscribe(:buffers)
    user = insert(:user)
    %{token: source_token} = source = insert(:source, user: user)
    %{token: backend_token} = backend = insert(:backend, user: user)

    start_supervised!(
      {BufferProducer,
       broadcast_interval: 100, backend_token: backend.token, source_token: source.token}
    )

    :timer.sleep(200)
    assert_receive {:buffers, ^source_token, ^backend_token, _payload}

    assert PubSubRates.Cache.get_cluster_buffers(source.token, backend.token) == 0
  end

  test "BufferProducer when discarding will display source name" do
    user = insert(:user)
    source = insert(:source, user: user)

    pid =
      start_supervised!(
        {BufferProducer,
         broadcast_interval: 100, backend_token: nil, source_token: source.token, buffer_size: 10}
      )

    items = for _ <- 1..100, do: "test"

    captured =
      capture_log(fn ->
        send(pid, {:add_to_buffer, items})
        :timer.sleep(100)
      end)

    assert captured =~ source.name
    assert captured =~ Atom.to_string(source.token)
  end
end
