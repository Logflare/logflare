defmodule Logflare.Utils.SSRF do
  @moduledoc """
  Helpers for detecting private/reserved IP addresses to prevent SSRF.
  """

  @typep ipv4 :: :inet.ip4_address()
  @typep ipv6 :: :inet.ip6_address()

  defguardp is_private_ipv4(a, b, c, d)
            when a == 127 or
                   a == 0 or
                   (a == 169 and b == 254) or
                   a == 10 or
                   (a == 172 and b >= 16 and b <= 31) or
                   (a == 192 and b == 168) or
                   (a == 100 and b >= 64 and b <= 127) or
                   (a == 255 and b == 255 and c == 255 and d == 255)

  @doc "Returns true if the address is loopback, link-local, RFC1918, CGNAT, or broadcast."
  @spec private_ip?(:inet.ip_address()) :: boolean()
  def private_ip?({a, b, c, d}) when is_private_ipv4(a, b, c, d), do: true
  def private_ip?({_, _, _, _}), do: false
  def private_ip?(addr), do: private_ipv6?(addr)

  @doc """
  Resolves `host` and returns the first safe IP address, or an error if the
  host resolves to any private/reserved address or cannot be resolved at all.

  Used to obtain an IP to connect to directly, eliminating DNS re-resolution.
  """
  @spec safe_resolve(String.t() | nil) :: {:ok, :inet.ip_address()} | {:error, String.t()}
  def safe_resolve(nil), do: {:error, "invalid host"}

  def safe_resolve(host) do
    charlist = String.to_charlist(host)

    case :inet.parse_address(charlist) do
      {:ok, addr} ->
        if private_ip?(addr),
          do: {:error, "URL must not target private or reserved IP addresses"},
          else: {:ok, addr}

      {:error, _} ->
        resolve_hostname(charlist)
    end
  end

  @doc "Formats an IP address tuple as a URL host component (IPv6 wrapped in brackets)."
  @spec url_host(:inet.ip_address()) :: String.t()
  def url_host(addr) when tuple_size(addr) == 8,
    do: "[#{addr |> :inet.ntoa() |> List.to_string()}]"

  def url_host(addr), do: addr |> :inet.ntoa() |> List.to_string()

  defp resolve_hostname(charlist) do
    ipv4 = safe_family(charlist, :inet)
    ipv6 = safe_family(charlist, :inet6)

    case {ipv4, ipv6} do
      {{:error, _} = err, _} -> err
      {_, {:error, _} = err} -> err
      {{:ok, _} = ok, _} -> ok
      {:unresolved, {:ok, _} = ok} -> ok
      {:unresolved, :unresolved} -> {:error, "could not resolve webhook destination host"}
    end
  end

  defp safe_family(charlist, family) do
    case :inet.getaddrs(charlist, family) do
      {:ok, addrs} ->
        if Enum.any?(addrs, &private_ip?/1),
          do: {:error, "URL must not target private or reserved IP addresses"},
          else: {:ok, List.first(addrs)}

      {:error, _} ->
        :unresolved
    end
  end

  @spec private_ipv6?(ipv6()) :: boolean()
  defp private_ipv6?({0, 0, 0, 0, 0, 0, 0, 1}), do: true
  defp private_ipv6?({0, 0, 0, 0, 0, 0, 0, 0}), do: true
  # Link-local fe80::/10
  defp private_ipv6?({a, _, _, _, _, _, _, _}) when (a &&& 0xFFC0) == 0xFE80, do: true
  # Unique local fc00::/7 (covers fd00::/8 which includes AWS IMDS fd00:ec2::254)
  defp private_ipv6?({a, _, _, _, _, _, _, _}) when (a &&& 0xFE00) == 0xFC00, do: true
  defp private_ipv6?({0, 0, 0, 0, 0, 0xFFFF, ab, cd}),
    do: private_ip?({ab >>> 8, ab &&& 0xFF, cd >>> 8, cd &&& 0xFF})

  defp private_ipv6?({_, _, _, _, _, _, _, _}), do: false
end
