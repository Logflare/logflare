defmodule Logflare.Backends.Adaptor.TCPAdaptor.Pool do
  @behaviour NimblePool

  def start_link(config) do
    NimblePool.start_link(worker: {__MODULE__, config}, lazy: true)
  end

  def send(pool, message) do
    result =
      NimblePool.checkout!(pool, :send, fn _from, socket ->
        {:gen_tcp.send(socket, message), socket}
      end)

    case result do
      :ok -> :ok
      {:error, reason} -> raise "Failed to send message to TCP backend - #{inspect(reason)}"
    end
  end

  @impl NimblePool
  def init_worker(%{host: host, port: port} = state) do
    this = self()

    # TODO: Add SSL support there
    async = fn ->
      connect_result =
        :gen_tcp.connect(to_charlist(host), port,
          mode: :binary,
          nodelay: true
        )

      socket =
        case connect_result do
          {:ok, socket} ->
            socket

          {:error, reason} ->
            raise "Failed to connect to TCP backend at #{host}:#{port} - #{inspect(reason)}"
        end

      case :gen_tcp.controlling_process(socket, this) do
        :ok -> :ok
        {:error, reason} -> raise "Failed to set controlling process - #{inspect(reason)}"
      end

      socket
    end

    {:async, async, state}
  end

  @impl NimblePool
  def handle_checkout(_command, _from, socket, state) do
    {:ok, socket, socket, state}
  end

  @impl NimblePool
  def handle_info({:tcp, socket, _}, socket) do
    {:ok, socket}
  end

  def handle_info({:tcp_closed, socket}, socket) do
    {:remove, :closed}
  end

  def handle_info(unexpected_message, _socket) do
    raise "Unexpected message in TCPAdaptor: #{inspect(unexpected_message)}"
  end
end
