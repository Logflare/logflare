defmodule Logflare.Backends.Adaptor.TCPAdaptor.Pool do
  @behaviour NimblePool

  def start_link(config) do
    NimblePool.start_link(worker: {__MODULE__, config}, lazy: true)
  end

  def send(pool, message) do
    NimblePool.checkout!(pool, :send, fn _from, socket ->
      result = :gen_tcp.send(socket, message)

      {result, result}
    end)
  end

  @impl NimblePool
  def init_worker(%{host: host, port: port} = state) do
    this = self()

    # TODO: Add SSL support there
    async = fn ->
      {:ok, socket} =
        :gen_tcp.connect(to_charlist(host), port,
          mode: :binary,
          nodelay: true
        )

      :gen_tcp.controlling_process(socket, this)

      socket
    end

    {:async, async, state}
  end

  @impl NimblePool
  def handle_checkout(_command, _from, socket, state) do
    {:ok, socket, socket, state}
  end

  @impl NimblePool
  # Ignore any data sent over the socket
  def handle_info({:tcp, socket, _}, socket),
    do: {:ok, socket}

  def handle_info({:tcp_closed, socket}, socket),
    do: {:remove, "connection closed"}

  def handle_info(_other, socket) do
    {:ok, socket}
  end
end
