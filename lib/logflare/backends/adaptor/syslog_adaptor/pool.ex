defmodule Logflare.Backends.Adaptor.SyslogAdaptor.Pool do
  @moduledoc false
  import Kernel, except: [send: 2]
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

    connect_opts =
      [mode: :binary, active: false, nodelay: true]
      |> maybe_configure_ssl(transport, config, host)

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
  @backoff_base to_timeout(millisecond: 100)
  @backoff_max to_timeout(second: 5)

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
          raise "failed to connect to TCP backend at tcp://#{host}:#{port} - #{inspect(reason)}"
      end
    end
  end

  defp send_data(socket, data) when is_port(socket), do: :gen_tcp.send(socket, data)
  defp send_data(socket, data), do: :ssl.send(socket, data)

  defp close(socket) when is_port(socket), do: :gen_tcp.close(socket)
  defp close(socket), do: :ssl.close(socket)

  defp delay_connect(failure_count) do
    factor = Integer.pow(2, failure_count)
    max_sleep = min(@backoff_max, @backoff_base * factor)
    sleep_for = :rand.uniform(max_sleep)
    Process.sleep(sleep_for)
  end

  defp maybe_configure_ssl(opts, :gen_tcp, _config, _host), do: opts

  defp maybe_configure_ssl(opts, :ssl, config, host) do
    ssl_opts = [
      server_name_indication: String.to_charlist(host),
      verify: :verify_peer,
      depth: 100,
      customize_hostname_check: [
        match_fun: :public_key.pkix_verify_hostname_match_fun(:https)
      ]
    ]

    ca_cert = Map.get(config, :ca_cert)
    client_cert = Map.get(config, :client_cert)
    client_key = Map.get(config, :client_key)

    ssl_opts =
      ssl_opts
      |> add_cacerts(ca_cert)
      |> add_key_cert(client_cert, client_key)

    opts ++ ssl_opts
  end

  defp add_cacerts(opts, nil) do
    [{:cacerts, :public_key.cacerts_get()} | opts]
  end

  defp add_cacerts(opts, pem_str) do
    certs =
      pem_str
      |> :public_key.pem_decode()
      |> Enum.map(fn {_, der, _} -> der end)

    if certs == [] do
      raise "CA Cert PEM contained no certificates"
    end

    [{:cacerts, certs} | opts]
  end

  defp add_key_cert(opts, nil, nil), do: opts
  defp add_key_cert(_opts, nil, _key), do: raise("client_key provided without client_cert")
  defp add_key_cert(_opts, _cert, nil), do: raise("client_cert provided without client_key")

  defp add_key_cert(opts, cert_pem, key_pem) do
    cert_der =
      case :public_key.pem_decode(cert_pem) do
        [{_type, der, _} | _] -> der
        [] -> raise "Client Certificate is invalid (no PEM entries found)."
      end

    key =
      case :public_key.pem_decode(key_pem) do
        [{type, der, _} | _] -> {type, der}
        [] -> raise "Client Key is invalid (no PEM entries found)."
      end

    [{:cert, cert_der}, {:key, key} | opts]
  end
end
