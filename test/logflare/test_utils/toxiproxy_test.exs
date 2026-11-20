defmodule Logflare.TestUtils.ToxiproxyTest do
  use ExUnit.Case, async: true

  alias Logflare.TestUtils.Toxiproxy

  @moduletag :toxiproxy

  setup do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, {_address, port}} = :inet.sockname(listener)
    on_exit(fn -> :gen_tcp.close(listener) end)

    {:ok, listener: listener, upstream: "127.0.0.1:#{port}"}
  end

  test "proxies have isolated ports, faults, and cleanup", %{
    listener: listener,
    upstream: upstream
  } do
    on_exit(:verify_proxies_deleted, fn -> :ok end)
    first = Toxiproxy.create!(upstream)
    second = Toxiproxy.create!(upstream)

    on_exit(:verify_proxies_deleted, fn ->
      for proxy <- [first, second] do
        assert {:error, :econnrefused} =
                 :gen_tcp.connect(~c"127.0.0.1", proxy.port, [:binary, active: false], 1_000)
      end
    end)

    refute first.name == second.name
    refute first.port == second.port
    assert first.port > 0
    assert second.port > 0

    {first_socket, first_task} = connection!(first, listener)
    roundtrip!(first_socket, "first")
    {second_socket, second_task} = connection!(second, listener)
    roundtrip!(second_socket, "second")

    assert :ok = Toxiproxy.enable!(first, false)
    assert_disconnected(first_socket)
    roundtrip!(second_socket, "still available")

    assert :ok = Toxiproxy.enable!(first, true)
    {restored_socket, restored_task} = connection!(first, listener)
    roundtrip!(restored_socket, "restored")

    close!(first_socket, first_task)
    close!(second_socket, second_task)
    close!(restored_socket, restored_task)
  end

  test "a downstream timeout stalls traffic until the toxic is removed", %{
    listener: listener,
    upstream: upstream
  } do
    proxy = Toxiproxy.create!(upstream)
    {socket, task} = connection!(proxy, listener)
    roundtrip!(socket, "before fault")

    assert %{"name" => "stall"} =
             Toxiproxy.add_toxic!(proxy, %{
               name: "stall",
               type: "timeout",
               stream: "downstream",
               attributes: %{timeout: 0}
             })

    assert :ok = :gen_tcp.send(socket, "during fault")
    assert {:error, :timeout} = :gen_tcp.recv(socket, 0, 100)

    assert :ok = Toxiproxy.remove_toxic!(proxy, "stall")
    close!(socket, task)
    {restored_socket, restored_task} = connection!(proxy, listener)
    roundtrip!(restored_socket, "after fault")
    close!(restored_socket, restored_task)
  end

  test "a reset disconnects an established connection and removal permits reconnection", %{
    listener: listener,
    upstream: upstream
  } do
    proxy = Toxiproxy.create!(upstream)
    {socket, task} = connection!(proxy, listener)
    roundtrip!(socket, "before reset")

    assert %{"name" => "reset"} =
             Toxiproxy.add_toxic!(proxy, %{
               name: "reset",
               type: "reset_peer",
               stream: "downstream",
               attributes: %{timeout: 0}
             })

    assert :ok = :gen_tcp.send(socket, "trigger reset")
    assert_disconnected(socket)
    close!(socket, task)

    assert :ok = Toxiproxy.remove_toxic!(proxy, "reset")
    {restored_socket, restored_task} = connection!(proxy, listener)
    roundtrip!(restored_socket, "after reset")
    close!(restored_socket, restored_task)
  end

  @spec connection!(Toxiproxy.proxy(), :gen_tcp.socket()) :: {:gen_tcp.socket(), Task.t()}
  defp connection!(proxy, listener) do
    task =
      Task.async(fn ->
        {:ok, socket} = :gen_tcp.accept(listener, 5_000)
        echo(socket)
      end)

    {:ok, socket} =
      :gen_tcp.connect(
        String.to_charlist(proxy.host),
        proxy.port,
        [:binary, active: false],
        1_000
      )

    {socket, task}
  end

  @spec echo(:gen_tcp.socket()) :: :ok
  defp echo(socket) do
    case :gen_tcp.recv(socket, 0) do
      {:ok, data} ->
        :ok = :gen_tcp.send(socket, data)
        echo(socket)

      {:error, reason} when reason in [:closed, :econnreset] ->
        :ok
    end
  end

  @spec roundtrip!(:gen_tcp.socket(), binary()) :: :ok
  defp roundtrip!(socket, data) do
    assert :ok = :gen_tcp.send(socket, data)
    assert {:ok, ^data} = :gen_tcp.recv(socket, byte_size(data), 1_000)
    :ok
  end

  @spec assert_disconnected(:gen_tcp.socket()) :: :ok
  defp assert_disconnected(socket) do
    assert {:error, reason} = :gen_tcp.recv(socket, 0, 1_000)
    assert reason in [:closed, :econnreset]
    :ok
  end

  @spec close!(:gen_tcp.socket(), Task.t()) :: :ok
  defp close!(socket, task) do
    :ok = :gen_tcp.close(socket)
    :ok = Task.await(task, 5_000)
  end
end
