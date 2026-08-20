defmodule Logflare.Backends.Adaptor.HttpBased.SSRFProtection do
  @moduledoc false
  @behaviour Tesla.Middleware

  alias Logflare.Utils.SSRF

  @impl Tesla.Middleware
  def call(env, next, _opts) do
    uri = URI.parse(env.url)

    case SSRF.safe_resolve(uri.host) do
      {:ok, addr} when uri.scheme == "http" ->
        # Rewrite the URL to the resolved IP so Finch connects directly without
        # re-resolving DNS. Set the Host header so the server sees the original
        # hostname (required by HTTP/1.1 and virtual hosting).
        ip_host = SSRF.url_host(addr)
        rewritten = URI.to_string(%{uri | host: ip_host, authority: nil})
        headers = List.keystore(env.headers, "host", 0, {"host", host_header(uri)})
        Tesla.run(%{env | url: rewritten, headers: headers}, next)

      {:ok, _addr} ->
        # HTTPS: cannot safely rewrite without per-request SNI control;
        # TLS certificate validation provides a secondary defence.
        Tesla.run(env, next)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec host_header(URI.t()) :: String.t()
  defp host_header(%URI{authority: authority, userinfo: userinfo}) when is_binary(authority) do
    # URI.host omits IPv6 brackets and the explicit port, so preserve the URI authority while
    # removing userinfo, which must not be included in the Host header.
    if userinfo do
      String.replace_prefix(authority, "#{userinfo}@", "")
    else
      authority
    end
  end
end
