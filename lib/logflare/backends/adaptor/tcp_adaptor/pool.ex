defmodule Logflare.Backends.Adaptor.TCPAdaptor.Pool do
  @moduledoc false
  import Kernel, except: [send: 2]
  require Record
  @behaviour NimblePool

  alias Logflare.Backends.Adaptor.TCPAdaptor.SSL

  def start_link(opts) do
    config = Keyword.fetch!(opts, :config)
    name = Keyword.fetch!(opts, :name)
    NimblePool.start_link(worker: {__MODULE__, config}, lazy: true, name: name)
  end

  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
  end

  def send(pool, message) do
    NimblePool.checkout!(pool, :checkout, fn _from, socket ->
      case send_data(socket, message) do
        :ok -> {:ok, :ok}
        {:error, _reason} = error -> {error, :close}
      end
    end)
  end

  @impl NimblePool
  def init_worker(pool_state) do
    protocol = if pool_state[:tls], do: :ssl, else: :tcp
    {:async, async_connect(protocol, pool_state, self()), pool_state}
  end

  @impl NimblePool
  def handle_checkout(:checkout, _from, socket, pool_state) do
    {:ok, socket, socket, pool_state}
  end

  @impl NimblePool
  def handle_checkin(:ok, _from, socket, pool_state) do
    {:ok, socket, pool_state}
  end

  def handle_checkin(:close, _from, _socket, pool_state) do
    {:remove, :closed, pool_state}
  end

  @impl NimblePool
  def terminate_worker(_reason, socket, pool_state) do
    close(socket)
    {:ok, pool_state}
  end

  @default_tcp_opts [mode: :binary, active: false, nodelay: true]

  defp async_connect(:tcp, config, owner) do
    %{host: host, port: port} = config

    fn ->
      timeout = to_timeout(second: 15)
      connect(:tcp, host, port, @default_tcp_opts, timeout, owner)
    end
  end

  defp async_connect(:ssl, config, owner) do
    %{host: host, port: port} = config

    cacerts =
      if ca_cert = config[:ca_cert] do
        ca_cert
        |> :public_key.pem_decode()
        |> Enum.map(fn {_, der, _} -> der end)
      else
        []
      end

    cert =
      if client_cert = config[:client_cert] do
        [{_, der, _}] = :public_key.pem_decode(client_cert)
        der
      end

    key =
      if client_key = config[:client_key] do
        [{type, der, _}] = :public_key.pem_decode(client_key)
        {type, der}
      end

    fn ->
      ssl_opts = @default_tcp_opts ++ [cacerts: cacerts, cert: cert, key: key]
      opts = SSL.opts(host, ssl_opts)
      timeout = to_timeout(second: 15)
      connect(:ssl, host, port, opts, timeout, owner)
    end
  end

  defp connect(:tcp, host, port, opts, timeout, owner) do
    host = String.to_charlist(host)

    case :gen_tcp.connect(host, port, opts, timeout) do
      {:ok, socket} ->
        case :gen_tcp.controlling_process(socket, owner) do
          :ok -> socket
          {:error, reason} -> raise "Failed to set controlling process - #{inspect(reason)}"
        end

      {:error, reason} ->
        raise "Failed to connect to TCP backend at tcp://#{host}:#{port} - #{inspect(reason)}"
    end
  end

  defp connect(:ssl, host, port, opts, timeout, owner) do
    host = String.to_charlist(host)

    case :ssl.connect(host, port, opts, timeout) do
      {:ok, socket} ->
        case :ssl.controlling_process(socket, owner) do
          :ok -> socket
          {:error, reason} -> raise "Failed to set controlling process - #{inspect(reason)}"
        end

      {:error, reason} ->
        raise "Failed to connect to TCP backend at tls://#{host}:#{port} - #{inspect(reason)}"
    end
  end

  defp send_data(socket, data) when is_port(socket), do: :gen_tcp.send(socket, data)
  defp send_data(socket, data), do: :ssl.send(socket, data)

  defp close(socket) when is_port(socket), do: :gen_tcp.close(socket)
  defp close(socket), do: :ssl.close(socket)
end
