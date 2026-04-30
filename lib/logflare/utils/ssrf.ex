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
