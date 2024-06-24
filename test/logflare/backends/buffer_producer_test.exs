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
      start_supervised!(
        {BufferProducer,
         active_broadcast_interval: 100, idle_broadcast_interval: 100, source_token: source.token}
      )

    send(pid, {:add_to_buffer, [:something]})
    :timer.sleep(300)
    assert PubSubRates.Cache.get_cluster_buffers(source_token) == 1
  end

  test "BufferProducer broadcasts every n seconds with backend differentiation" do
    PubSubRates.subscribe(:buffers)
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)

    pid =
      start_supervised!(
        {BufferProducer,
         idle_broadcast_interval: 100,
         active_broadcast_interval: 100,
         backend_token: backend.token,
         source_token: source.token}
      )

    send(pid, {:add_to_buffer, [:something]})
    :timer.sleep(200)
    assert PubSubRates.Cache.get_cluster_buffers(source.token, backend.token) == 1
  end

  test "BufferProducer when discarding will display source name" do
    user = insert(:user)
    source = insert(:source, user: user)

    pid =
      start_supervised!(
        {BufferProducer,
         active_broadcast_interval: 100,
         backend_token: nil,
         source_token: source.token,
         buffer_size: 10}
      )

    items = for _ <- 1..100, do: "test"

    captured =
      capture_log(fn ->
        send(pid, {:add_to_buffer, items})
        :timer.sleep(100)
        send(pid, {:add_to_buffer, items})
        :timer.sleep(100)
      end)

    assert captured =~ source.name
    assert captured =~ Atom.to_string(source.token)
    # log only once
    assert count_substrings(captured, source.name) == 1
  end

  def count_substrings(string, substring) do
    regex = Regex.compile!(substring)

    Regex.scan(regex, string)
    |> length()
  end

  test "BufferProducer idle broadcast interval" do
    PubSubRates.subscribe(:buffers)
    user = insert(:user)
    source = insert(:source, user: user)

    pid =
      start_supervised!(
        {BufferProducer,
         idle_broadcast_interval: 500,
         active_broadcast_interval: 100,
         backend_token: nil,
         source_token: source.token,
         buffer_size: 1000}
      )

    items = for _ <- 1..100, do: "test"

    send(pid, {:add_to_buffer, items})
    :timer.sleep(400)
    assert PubSubRates.Cache.get_cluster_buffers(source.token, nil) != 0

    GenStage.stream([{pid, max_demand: 100}])
    |> Enum.take(5)

    :timer.sleep(400)
    assert PubSubRates.Cache.get_cluster_buffers(source.token, nil) == 0
  end
end
