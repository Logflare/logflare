defmodule Logflare.Backends.Adaptor.HttpBased.SSRFProtection do
  @moduledoc """
  Rejects requests targeting private or reserved IP addresses.

  Accepts an `:allow_private` middleware option so individual pipelines can opt
  out of the check — used by backends whose destination is legitimately on a
  private network (e.g. a self-hosted service reachable only over the internal
  network, or a docker-compose service on loopback during integration tests).

  Defaults to enforcing. Only disable it for pipelines whose destination is not
  user-controlled, since the check is what prevents a user-supplied URL from
  reaching internal services.
  """
  @behaviour Tesla.Middleware

  alias Logflare.Utils.SSRF

  @impl Tesla.Middleware
  def call(env, next, opts) do
    if Keyword.get(List.wrap(opts), :allow_private, false) do
      Tesla.run(env, next)
    else
      enforce(env, next)
    end
  end

  defp enforce(env, next) do
    uri = URI.parse(env.url)

    case SSRF.safe_resolve(uri.host) do
      {:ok, addr} when uri.scheme == "http" ->
        # Rewrite the URL to the resolved IP so Finch connects directly without
        # re-resolving DNS. Set the Host header so the server sees the original
        # hostname (required by HTTP/1.1 and virtual hosting).
        ip_host = SSRF.url_host(addr)
        rewritten = URI.to_string(%{uri | host: ip_host, authority: nil})
        headers = List.keystore(env.headers, "host", 0, {"host", uri.host})
        Tesla.run(%{env | url: rewritten, headers: headers}, next)

      {:ok, _addr} ->
        # HTTPS: cannot safely rewrite without per-request SNI control;
        # TLS certificate validation provides a secondary defence.
        Tesla.run(env, next)

      {:error, reason} ->
        {:error, reason}
    end
  end
end
