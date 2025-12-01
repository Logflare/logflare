defmodule Logflare.Backends.Adaptor.TCPAdaptor.Pool do
  @moduledoc false
  @behaviour NimblePool

  def start_link(opts) do
    config = Keyword.fetch!(opts, :config)
    name = Keyword.fetch!(opts, :name)
    NimblePool.start_link(worker: {__MODULE__, config}, lazy: true, name: name)
  end

  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
  end

  def send(pool, message) do
    result =
      NimblePool.checkout!(pool, :send, fn _from, socket ->
        res =
          if is_port(socket) do
            :gen_tcp.send(socket, message)
          else
            :ssl.send(socket, message)
          end

        {res, socket}
      end)

    case result do
      :ok -> :ok
      {:error, reason} -> raise "Failed to send message to TCP backend - #{inspect(reason)}"
    end
  end

  @impl NimblePool
  def init_worker(%{host: host, port: port} = state) do
    this = self()

    async = fn ->
      connect_result =
        if state[:tls] do
          opts = [
            mode: :binary,
            nodelay: true,
            verify: :verify_peer,
            depth: 3,
            cacerts: decode_cacerts(state[:ca_cert]),
            cert: decode_cert(state[:client_cert]),
            key: decode_key(state[:client_key])
          ]

          :ssl.connect(to_charlist(host), port, opts)
        else
          :gen_tcp.connect(to_charlist(host), port,
            mode: :binary,
            nodelay: true
          )
        end

      socket =
        case connect_result do
          {:ok, socket} ->
            socket

          {:error, reason} ->
            raise "Failed to connect to TCP backend at #{host}:#{port} - #{inspect(reason)}"
        end

      case set_controlling_process(socket, this, state[:tls]) do
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

  defp set_controlling_process(socket, pid, true), do: :ssl.controlling_process(socket, pid)
  defp set_controlling_process(socket, pid, _), do: :gen_tcp.controlling_process(socket, pid)

  defp decode_cacerts(nil), do: []

  defp decode_cacerts(pem) do
    :public_key.pem_decode(pem)
    |> Enum.map(fn {_, der, _} -> der end)
  end

  defp decode_cert(nil), do: nil

  defp decode_cert(pem) do
    [{_, der, _}] = :public_key.pem_decode(pem)
    der
  end

  defp decode_key(nil), do: nil

  defp decode_key(pem) do
    [{type, der, _}] = :public_key.pem_decode(pem)
    {type, der}
  end
end
