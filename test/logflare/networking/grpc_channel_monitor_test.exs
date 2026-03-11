defmodule Logflare.Networking.GrpcChannelMonitorTest do
  # async: false required because GRPC.Stub stubs need set_mimic_global
  use ExUnit.Case, async: false

  import Mimic

  alias Logflare.Networking.GrpcChannelMonitor
  alias Logflare.TestUtils

  setup :set_mimic_global
  setup :verify_on_exit!

  setup do
    registry = Module.concat(__MODULE__, Registry)
    # :listeners requires named processes; register test process under a unique atom
    listener = Module.concat(__MODULE__, Listener)
    Process.register(self(), listener)
    start_supervised!({Registry, keys: :unique, name: registry, listeners: [listener]})
    channel = %GRPC.Channel{}
    %{registry: registry, channel: channel}
  end

  defp start_monitor(registry, url \\ "https://example.com") do
    start_supervised!({GrpcChannelMonitor, {0, url, registry}})
  end

  # Succeeds on the first call, fails on all subsequent ones
  defp once_then_fail(channel) do
    counter = :counters.new(1, [])

    fn _url, _opts ->
      n = :counters.get(counter, 1)
      :counters.add(counter, 1, 1)
      if n == 0, do: {:ok, channel}, else: {:error, :econnrefused}
    end
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

    expect(GRPC.Stub, :connect, fn _url, _opts ->
      send(test_pid, :connect_attempt)
      {:error, :econnrefused}
    end)

    pid = start_monitor(registry)

    assert_receive :connect_attempt
    IO.puts("A")
    assert :sys.get_state(pid).backoff == 2_000

    refute_received {:register, ^registry, 0, _partition, _channel}

    expect(GRPC.Stub, :connect, fn _url, _opts ->
      send(test_pid, :connect_attempt_2)
      {:error, :econnrefused}
    end)

    IO.puts("B")
    send(pid, :connect)
    assert_receive :connect_attempt_2

    IO.puts("C")

    TestUtils.retry_assert(fn ->
      assert :sys.get_state(pid).backoff == 30_000
    end)

    IO.puts("D")
    refute_received {:register, ^registry, 0, _partition, _channel}
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
    test ":connection_down unregisters, reconnects, resets backoff", %{
      registry: registry,
      channel: channel
    } do
      stub(GRPC.Stub, :connect, once_then_fail(channel))

      pid = start_monitor(registry)
      assert_receive {:register, ^registry, 0, _partition, ^channel}

      :sys.replace_state(pid, fn state -> %{state | backoff: 10_000} end)
      send(pid, {:elixir_grpc, :connection_down, make_ref()})

      assert_receive {:unregister, ^registry, 0, _partition}

      TestUtils.retry_assert(fn ->
        state = :sys.get_state(pid)
        assert state.connected? == false
        # backoff reset to @min_backoff (1_000) then doubled after failed reconnect
        assert state.backoff == 2_000
      end)
    end

    test "{:EXIT, pid, reason} triggers same disconnection behaviour", %{
      registry: registry,
      channel: channel
    } do
      stub(GRPC.Stub, :connect, once_then_fail(channel))

      pid = start_monitor(registry)
      assert_receive {:register, ^registry, 0, _partition, ^channel}

      send(pid, {:EXIT, spawn(fn -> :ok end), :normal})

      assert_receive {:unregister, ^registry, 0, _partition}
      assert :sys.get_state(pid).connected? == false
    end
  end
end
