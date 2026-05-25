defmodule Logflare.Utils.SSRF do
  @moduledoc """
  Helpers for detecting private/reserved IP addresses to prevent SSRF.
  """

  import Logflare.Utils.Guards, only: [is_non_empty_binary: 1]

  @private_ranges Enum.map(
                    [
                      # IPv4
                      # all-zeros
                      "0.0.0.0/8",
                      # RFC 1918 private
                      "10.0.0.0/8",
                      # CGNAT
                      "100.64.0.0/10",
                      # loopback
                      "127.0.0.0/8",
                      # link-local / AWS IMDS
                      "169.254.0.0/16",
                      # RFC 1918 private
                      "172.16.0.0/12",
                      # RFC 1918 private
                      "192.168.0.0/16",
                      # broadcast
                      "255.255.255.255/32",
                      # IPv6
                      # all-zeros
                      "::/128",
                      # loopback
                      "::1/128",
                      # IPv4-mapped IPv6
                      "::ffff:0:0/96",
                      # unique-local (covers fd00::/8 = AWS IMDS fd00:ec2::254)
                      "fc00::/7",
                      # link-local
                      "fe80::/10"
                    ],
                    &InetCidr.parse_cidr!/1
                  )

  @private_ip_error "URL must not target private or reserved IP addresses"

  @doc "Returns true if the address is loopback, link-local, RFC1918, CGNAT, broadcast, or reserved."
  @spec private_ip?(:inet.ip_address()) :: boolean()
  def private_ip?(addr) when is_tuple(addr) do
    Enum.any?(@private_ranges, &InetCidr.contains?(&1, addr))
  end

  @doc """
  Resolves `host` and returns the first safe IP address, or an error if the
  host resolves to any private/reserved address or cannot be resolved at all.

  Used to obtain an IP to connect to directly, eliminating DNS re-resolution.
  """
  @spec safe_resolve(String.t() | nil) :: {:ok, :inet.ip_address()} | {:error, String.t()}
  def safe_resolve(host) when is_non_empty_binary(host) do
    charlist = String.to_charlist(host)

    with {:ok, addr} <- :inet.parse_address(charlist),
         {:private, false} <- {:private, private_ip?(addr)} do
      {:ok, addr}
    else
      {:error, _} -> resolve_hostname(charlist)
      {:private, true} -> {:error, @private_ip_error}
    end
  end

  def safe_resolve(_), do: {:error, "invalid host"}

  @doc "Formats an IP address tuple as a URL host component (IPv6 wrapped in brackets)."
  @spec url_host(:inet.ip_address()) :: String.t()
  def url_host(addr) when tuple_size(addr) == 8,
    do: "[#{addr |> :inet.ntoa() |> List.to_string()}]"

  def url_host(addr), do: addr |> :inet.ntoa() |> List.to_string()

  defp resolve_hostname(charlist) do
    with :unresolved <- resolve_hostname(charlist, :inet),
         :unresolved <- resolve_hostname(charlist, :inet6) do
      {:error, "could not resolve webhook destination host"}
    else
      other -> other
    end
  end

  defp resolve_hostname(charlist, family) do
    with {:ok, addrs} <- :inet.getaddrs(charlist, family),
         {:private, false} <- {:private, Enum.any?(addrs, &private_ip?/1)} do
      {:ok, List.first(addrs)}
    else
      {:private, true} -> {:error, @private_ip_error}
      {:error, _} -> :unresolved
    end
  end
end
