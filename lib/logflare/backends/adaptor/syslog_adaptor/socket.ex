defmodule Logflare.Backends.Adaptor.SyslogAdaptor.Socket do
  @moduledoc false

  alias Logflare.Utils.SSRF

  @typep address :: :inet.ip_address() | charlist()
  @typep reason :: :closed | :timeout | :inet.posix() | :ssl.reason() | {:ssrf, String.t()}
  @type socket :: :gen_tcp.socket() | :ssl.sslsocket()

  # see https://www.erlang.org/doc/apps/kernel/inet#setopts/2 for details
  @default_transport_opts mode: :binary,
                          packet: :raw,
                          active: true,
                          nodelay: true,
                          send_timeout: to_timeout(second: 5),
                          send_timeout_close: true

  @spec connect(map, timeout) :: {:ok, socket} | {:error, reason}
  def connect(config, timeout) do
    host = Map.fetch!(config, :host)
    port = Map.fetch!(config, :port)

    with {:ok, addresses} <- resolve_addresses(host) do
      connect_addresses(addresses, config, host, port, deadline(timeout), {:error, :einval})
    end
  end

  def send(socket, data) when is_port(socket), do: :gen_tcp.send(socket, data)
  def send(socket, data), do: :ssl.send(socket, data)

  def close(socket) when is_port(socket), do: :gen_tcp.close(socket)
  def close(socket), do: :ssl.close(socket)

  def controlling_process(socket, pid) when is_port(socket) do
    :gen_tcp.controlling_process(socket, pid)
  end

  def controlling_process(socket, pid), do: :ssl.controlling_process(socket, pid)

  def stream(socket, {tag, socket}) when tag in [:tcp_closed, :ssl_closed], do: {:error, :closed}

  def stream(socket, {tag, socket, reason}) when tag in [:tcp_error, :ssl_error] do
    close(socket)
    {:error, reason}
  end

  def stream(_socket, _message), do: :ignore

  @spec resolve_addresses(String.t()) :: {:ok, [address]} | {:error, {:ssrf, String.t()}}
  defp resolve_addresses(host) do
    if Logflare.SingleTenant.single_tenant?() do
      {:ok, [String.to_charlist(host)]}
    else
      case SSRF.safe_resolve_all(host) do
        {:ok, addresses} -> {:ok, addresses}
        {:error, reason} -> {:error, {:ssrf, reason}}
      end
    end
  end

  @spec connect_addresses(
          [address],
          map,
          String.t(),
          :inet.port_number(),
          integer | :infinity,
          term
        ) ::
          {:ok, socket} | {:error, reason}
  defp connect_addresses([address | addresses], config, host, port, deadline, _last_error) do
    case connect_address(address, config, host, port, remaining_timeout(deadline)) do
      {:ok, _socket} = result ->
        result

      {:error, reason} = error when reason in [:einval, :timeout] ->
        error

      {:error, _reason} = error ->
        connect_addresses(addresses, config, host, port, deadline, error)
    end
  end

  defp connect_addresses([], _config, _host, _port, _deadline, last_error), do: last_error

  @spec connect_address(address, map, String.t(), :inet.port_number(), timeout) ::
          {:ok, socket} | {:error, reason}
  defp connect_address(address, config, host, port, timeout) do
    if Map.get(config, :tls) do
      opts = ssl_opts(@default_transport_opts, config, host)
      :ssl.connect(address, port, opts, timeout)
    else
      :gen_tcp.connect(address, port, @default_transport_opts, timeout)
    end
  end

  @spec deadline(timeout) :: integer | :infinity
  defp deadline(:infinity), do: :infinity
  defp deadline(timeout), do: System.monotonic_time(:millisecond) + timeout

  @spec remaining_timeout(integer | :infinity) :: timeout
  defp remaining_timeout(:infinity), do: :infinity

  defp remaining_timeout(deadline) do
    max(deadline - System.monotonic_time(:millisecond), 0)
  end

  defp ssl_opts(opts, config, host) do
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

  # changesets already ensure these are valid PEM strings,
  # so we can assume that if they're present, they decode correctly

  defp add_cacerts(opts, nil) do
    [{:cacerts, :public_key.cacerts_get()} | opts]
  end

  defp add_cacerts(opts, pem) do
    certs =
      pem
      |> :public_key.pem_decode()
      |> Enum.map(fn {_, der, _} -> der end)

    [{:cacerts, certs} | opts]
  end

  defp add_key_cert(opts, nil, nil), do: opts

  defp add_key_cert(opts, cert_pem, key_pem) do
    [{_cert_type, cert_der, _} | _] = :public_key.pem_decode(cert_pem)
    [{key_type, key_der, _} | _] = :public_key.pem_decode(key_pem)
    [{:cert, cert_der}, {:key, {key_type, key_der}} | opts]
  end
end
