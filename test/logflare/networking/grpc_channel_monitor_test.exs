defmodule Logflare.Networking.GrpcChannelMonitorTest do
  use ExUnit.Case, async: true

  import Mimic

  alias Logflare.Networking.GrpcChannelMonitor

  setup do
    test_pid = self()
    registry = Module.concat(__MODULE__, Registry)
    # :listeners requires named processes; register test process under a unique atom
    listener = Module.concat(__MODULE__, Listener)
    Process.register(test_pid, listener)
    start_supervised!({Registry, keys: :unique, name: registry, listeners: [listener]})
    channel = %GRPC.Channel{ref: make_ref()}

    send_after = fn _pid, :connect, timeout ->
      send(test_pid, {:send_after, timeout})
      make_ref()
    end

    Application.put_env(:logflare, GrpcChannelMonitor, send_after: send_after)
    on_exit(fn -> Application.put_env(:logflare, GrpcChannelMonitor, []) end)

    %{registry: registry, channel: channel, test_pid: test_pid}
  end

  defp start_monitor(registry, opts \\ []) do
    url = Keyword.get(opts, :url, "https://example.com")

    pid = start_supervised!({GrpcChannelMonitor, {0, url, registry}})
    allow(GRPC.Stub, self(), pid)

    if Keyword.get(opts, :connect?, true) do
      assert_receive {:send_after, 0}
      send(pid, :connect)
    end

    pid
  end

  test "successful connect", %{
    registry: registry,
    channel: channel
  } do
    expect(GRPC.Stub, :connect, fn _url, _opts -> {:ok, channel} end)

    _pid = start_monitor(registry)

    assert_receive {:register, ^registry, 0, _partition, ^channel}
  end

  test "failed connect", %{registry: registry} do
    test_pid = self()

    pid = start_monitor(registry, connect?: false)

    for timeout <- [0, 1, 2, 4, 8, 16, 30, 30] do
      expect(GRPC.Stub, :connect, fn _url, _opts ->
        send(test_pid, :connect_attempt)
        {:error, :econnrefused}
      end)

      send(pid, :connect)

      assert_receive :connect_attempt
      expected_timeout = timeout * 1000
      assert_receive {:send_after, ^expected_timeout}
      refute_received {:register, ^registry, 0, _partition, _channel}
    end
  end

  test "connect when already connected", %{registry: registry, channel: channel} do
    expect(GRPC.Stub, :connect, 1, fn _url, _opts -> {:ok, channel} end)

    pid = start_monitor(registry)
    assert_receive {:register, ^registry, 0, _partition, ^channel}

    send(pid, :connect)
    refute_receive {:register, ^registry, 0, _partition, _channel}

    assert [{^pid, ^channel}] = Registry.lookup(registry, 0)
  end

  describe "disconnection handling" do
    test ":connection_down", %{
      registry: registry,
      channel: channel
    } do
      expect(GRPC.Stub, :connect, fn _url, _opts -> {:ok, channel} end)
      expect(GRPC.Stub, :disconnect, fn ^channel -> {:ok, channel} end)

      pid = start_monitor(registry)
      allow(GRPC.Stub, self(), pid)
      send(pid, :connect)
      assert_receive {:register, ^registry, 0, _partition, ^channel}

      send(pid, {:elixir_grpc, :connection_down, channel.ref})

      assert_receive {:unregister, ^registry, 0, _partition}
      assert_receive {:send_after, 0}
    end

    test "{:EXIT, pid, reason}", %{
      registry: registry,
      channel: channel
    } do
      expect(GRPC.Stub, :connect, fn _url, _opts -> {:ok, channel} end)
      expect(GRPC.Stub, :disconnect, fn ^channel -> {:ok, channel} end)

      pid = start_monitor(registry)
      allow(GRPC.Stub, self(), pid)
      send(pid, :connect)
      assert_receive {:register, ^registry, 0, _partition, ^channel}

      send(pid, {:elixir_grpc, :connection_down, channel.ref})

      assert_receive {:unregister, ^registry, 0, _partition}
      assert_receive {:send_after, 0}
    end
  end
end
