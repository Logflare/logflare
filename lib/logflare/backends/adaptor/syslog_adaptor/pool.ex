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

  @spec send(NimblePool.pool(), iodata) :: :ok | {:error, error_reason}
        when error_reason: :closed | :inet.posix() | :ssl.reason()
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

    # see https://www.erlang.org/doc/apps/kernel/inet#setopts/2 for details
    connect_opts =
      [
        mode: :binary,
        packet: :raw,
        # enable async messages to catch closed sockets early
        active: true,
        # disable Nagle's algorithm for lower latency
        nodelay: true,
        # don't hang the worker if the inet driver queue fills up
        send_timeout: to_timeout(second: 5),
        send_timeout_close: true
        # NOTE: we might also want to add :inet6, :recbuf, :linder, :show_econnreset, etc.
      ]
      |> maybe_enable_keepalive()
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

  # NOTE: handle_info is O(N) (it calls the function for every worker in the pool for every message received).
  # Since Syslog is typically a "write-only" protocol where we don't expect return traffic, this is probably fine.
  # The only expected messages are tcp_closed / tcp_error, which are rare.
  @impl NimblePool
  def handle_info(message, socket)

  # swallow unexpected data
  def handle_info({tag, socket, _data}, socket) when tag in [:tcp, :ssl] do
    {:ok, socket}
  end

  # close and remove sockets on any error
  def handle_info({tag, socket, reason}, socket) when tag in [:tcp_error, :ssl_error] do
    # Mint closes on error so do we: https://github.com/elixir-mint/mint/blob/e28c85aad15d1f0cfcb1d5e4f4abada5f37f0f11/lib/mint/http1.ex#L533-L535
    close(socket)
    {:remove, reason}
  end

  # handle normal closure
  def handle_info({tag, socket}, socket) when tag in [:tcp_closed, :ssl_closed] do
    {:remove, :closed}
  end

  def handle_info(_message, socket) do
    {:ok, socket}
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

      # we need to try/catch since gen_tcp sometimes raises (e.g, on bad options)
      # and we don't want to exit without incrementing connect failures
      result =
        try do
          with {:ok, socket} = ok <-
                 transport.connect(host, port, connect_opts, @connect_timeout),
               :ok <- transport.controlling_process(socket, owner) do
            ok
          end
        catch
          _kind, reason -> {:error, reason}
        end

      case result do
        {:ok, socket} ->
          :atomics.put(connect_failures, 1, 0)
          socket

        {:error, reason} ->
          :atomics.add(connect_failures, 1, 1)

          raise "failed to connect to Syslog backend at #{host}:#{port}, reason: #{inspect(reason)}"
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

  defp maybe_enable_keepalive(opts) do
    case :os.type() do
      {:unix, :linux} ->
        Keyword.merge(opts,
          keepalive: true,
          # seconds of silence before sending a probe;
          # we need to set it since the default on linux is tcp_keepalive_time=7200, i.e. 2 hours
          keepidle: 15,
          # seconds between probes
          keepintvl: 5,
          # number of failed probes before killing the socket
          keepcnt: 3
        )

      _other ->
        opts
    end
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
