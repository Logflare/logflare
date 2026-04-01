defmodule Logflare.Backends.Adaptor.SyslogAdaptor.Pool do
  @moduledoc false

  import Kernel, except: [send: 2]

  alias Logflare.Backends

  @behaviour NimblePool

  @connect_timeout to_timeout(second: 15)

  # see https://www.erlang.org/doc/apps/kernel/inet#setopts/2 for details
  @default_transport_opts mode: :binary, packet: :raw, active: true, nodelay: true

  def start_link(opts) do
    backend_id = Keyword.fetch!(opts, :backend_id)
    name = Keyword.fetch!(opts, :name)
    NimblePool.start_link(worker: {__MODULE__, backend_id}, lazy: true, name: name)
  end

  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
  end

  @spec send(NimblePool.pool(), iodata) :: :ok | {:error, error_reason}
        when error_reason: :closed | :timeout | :inet.posix() | :ssl.reason()
  def send(pool, message) do
    NimblePool.checkout!(pool, :checkout, fn {pid, _ref}, worker ->
      with {:connected, socket, _config} = conn <- ensure_connected(worker, pid),
           :ok <- send_data(socket, message) do
        {:ok, {:ok, conn}}
      else
        {:error, reason} = error -> {error, {:remove, reason}}
      end
    end)
  end

  defp ensure_connected(worker, owner) do
    with {:connect, backend_id} <- worker do
      config = current_backend_config(backend_id)

      case connect(config, owner) do
        {:ok, socket} -> {:connected, socket, config}
        {:error, _reason} = error -> error
      end
    end
  end

  @impl NimblePool
  def init_pool(backend_id) do
    {:ok, backend_id}
  end

  @impl NimblePool
  def init_worker(backend_id) do
    {:ok, :idle, backend_id}
  end

  @impl NimblePool
  def handle_checkout(:checkout, _from, conn, backend_id) do
    case conn do
      :idle ->
        {:ok, {:connect, backend_id}, conn, backend_id}

      {:connected, _socket, backend_config} ->
        # if current backend config is the same as what it was when conn was opened,
        # return conn, otherwise remove it and try another one
        if backend_config == current_backend_config(backend_id) do
          {:ok, conn, conn, backend_id}
        else
          {:remove, :stale_config, backend_id}
        end
    end
  end

  @impl NimblePool
  def handle_checkin({:ok, conn}, _from, _prev_conn, backend_id) do
    {:ok, conn, backend_id}
  end

  def handle_checkin({:remove, reason}, _from, _conn, backend_id) do
    {:remove, reason, backend_id}
  end

  # NOTE: handle_info is called for every socket in the pool for every message received.
  # Since Syslog is typically a "write-only" protocol where we don't expect return traffic, this is probably fine.
  # The only expected messages are tcp_closed / tcp_error, which are rare.
  @impl NimblePool
  def handle_info(message, conn)

  # close and remove sockets on any error
  def handle_info({tag, socket, reason}, {:connected, socket, _config})
      when tag in [:tcp_error, :ssl_error] do
    # mint closes on error, so do we: https://github.com/elixir-mint/mint/blob/e28c85aad15d1f0cfcb1d5e4f4abada5f37f0f11/lib/mint/http1.ex#L533-L535
    close(socket)
    {:remove, reason}
  end

  # handle normal closure
  def handle_info({tag, socket}, {:connected, socket, _config})
      when tag in [:tcp_closed, :ssl_closed] do
    {:remove, :closed}
  end

  def handle_info(_message, conn) do
    {:ok, conn}
  end

  @impl NimblePool
  def terminate_worker(_reason, conn, backend_id) do
    with {:connected, socket, _config} <- conn, do: close(socket)
    {:ok, backend_id}
  end

  defp connect(backend_config, owner) do
    host = Map.fetch!(backend_config, :host)
    port = Map.fetch!(backend_config, :port)
    transport = if Map.get(backend_config, :tls), do: :ssl, else: :gen_tcp
    opts = maybe_configure_ssl(@default_transport_opts, transport, backend_config, host)
    host = String.to_charlist(host)

    with {:ok, socket} <- transport.connect(host, port, opts, @connect_timeout) do
      case transport.controlling_process(socket, owner) do
        :ok ->
          {:ok, socket}

        {:error, _reason} = error ->
          close(socket)
          error
      end
    end
  catch
    :exit, :badarg -> {:error, :badarg}
  end

  defp send_data(socket, data) when is_port(socket), do: :gen_tcp.send(socket, data)
  defp send_data(socket, data), do: :ssl.send(socket, data)

  defp close(socket) when is_port(socket), do: :gen_tcp.close(socket)
  defp close(socket), do: :ssl.close(socket)

  defp current_backend_config(backend_id) do
    if backend = Backends.Cache.get_backend(backend_id) do
      backend.config
    else
      raise "missing backend #{backend_id}"
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
