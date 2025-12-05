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
  def init_pool(config) do
    host = Map.fetch!(config, :host)
    port = Map.fetch!(config, :port)
    transport = if Map.get(config, :tls), do: :ssl, else: :gen_tcp

    extra_connect_opts =
      if transport == :ssl do
        cacerts =
          if ca_cert = Map.get(config, :ca_cert) do
            ca_cert
            |> :public_key.pem_decode()
            |> Enum.map(fn {_, der, _} -> der end)
          else
            :public_key.cacerts_get()
          end

        opts = [cacerts: cacerts]

        opts =
          if client_cert = Map.get(config, :client_cert) do
            [{_, der, _}] = :public_key.pem_decode(client_cert)
            [{:cert, der} | opts]
          else
            opts
          end

        opts =
          if client_key = Map.get(config, :client_key) do
            [{type, der, _}] = :public_key.pem_decode(client_key)
            [{:key, {type, der}} | opts]
          else
            opts
          end

        SSL.opts(host, opts)
      else
        []
      end

    connect_opts = [mode: :binary, active: false, nodelay: true] ++ extra_connect_opts

    pool_state = %{
      host: String.to_charlist(host),
      port: port,
      transport: transport,
      connect_opts: connect_opts,
      connect_failures: :atomics.new(1, signed: false)
    }

    {:ok, pool_state}
  end

  @impl NimblePool
  def init_worker(pool_state) do
    {:async, async_connect(pool_state, self()), pool_state}
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

  @connect_timeout to_timeout(second: 15)

  defp async_connect(pool_state, owner) do
    %{
      host: host,
      port: port,
      transport: transport,
      connect_opts: connect_opts,
      connect_failures: connect_failures
    } = pool_state

    fn ->
      failure_count = :atomics.get(connect_failures, 1)
      if failure_count > 0, do: delay_connect(failure_count)

      with {:ok, socket} <- transport.connect(host, port, connect_opts, @connect_timeout),
           :ok <- transport.controlling_process(socket, owner) do
        :atomics.put(connect_failures, 1, 0)
        socket
      else
        {:error, reason} ->
          :atomics.add(connect_failures, 1, 1)
          raise "Failed to connect to TCP backend at tcp://#{host}:port - #{inspect(reason)}"
      end
    end
  end

  defp send_data(socket, data) when is_port(socket), do: :gen_tcp.send(socket, data)
  defp send_data(socket, data), do: :ssl.send(socket, data)

  defp close(socket) when is_port(socket), do: :gen_tcp.close(socket)
  defp close(socket), do: :ssl.close(socket)

  @backoff_base to_timeout(millisecond: 100)
  @backoff_max to_timeout(second: 5)

  defp delay_connect(failure_count) do
    factor = Integer.pow(2, failure_count)
    max_sleep = min(@backoff_max, @backoff_base * factor)
    sleep_for = :rand.uniform(max_sleep)
    Process.sleep(sleep_for)
  end
end
