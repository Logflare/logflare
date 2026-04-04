defmodule Logflare.Backends.Adaptor.SyslogAdaptor.Socket do
  @moduledoc false

  @type socket :: :gen_tcp.socket() | :ssl.sslsocket()

  # see https://www.erlang.org/doc/apps/kernel/inet#setopts/2 for details
  @default_transport_opts mode: :binary, packet: :raw, active: true, nodelay: true

  @spec connect(map, timeout) :: {:ok, socket} | {:error, reason}
        when reason: :closed | :timeout | :inet.posix() | :ssl.reason()
  def connect(config, timeout) do
    host = config |> Map.fetch!(:host) |> String.to_charlist()
    port = Map.fetch!(config, :port)

    if Map.get(config, :tls) do
      opts = ssl_opts(@default_transport_opts, config, host)
      :ssl.connect(host, port, opts, timeout)
    else
      :gen_tcp.connect(host, port, @default_transport_opts, timeout)
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

  defp ssl_opts(opts, config, host) do
    ssl_opts = [
      server_name_indication: host,
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
