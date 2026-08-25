defmodule Logflare.Utils.SSRF do
  @moduledoc """
  Helpers for detecting private/reserved IP addresses to prevent SSRF.
  """

  import Logflare.Utils.Guards, only: [is_non_empty_binary: 1]

  alias Logflare.Utils.SSRF.TCP

  @blocked_ranges Enum.map(
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
                      # IETF protocol assignments
                      "192.0.0.0/24",
                      # documentation
                      "192.0.2.0/24",
                      # deprecated 6to4 relay anycast
                      "192.88.99.0/24",
                      # RFC 1918 private
                      "192.168.0.0/16",
                      # benchmarking
                      "198.18.0.0/15",
                      # documentation
                      "198.51.100.0/24",
                      # documentation
                      "203.0.113.0/24",
                      # multicast
                      "224.0.0.0/4",
                      # reserved
                      "240.0.0.0/4",
                      # broadcast
                      "255.255.255.255/32",
                      # IPv6
                      # outside the currently allocated global-unicast envelope
                      "::/3",
                      # all-zeros
                      "::/128",
                      # loopback
                      "::1/128",
                      # IPv4-mapped IPv6
                      "::ffff:0:0/96",
                      # NAT64 well-known prefix
                      "64:ff9b::/96",
                      # NAT64 local-use prefix
                      "64:ff9b:1::/48",
                      # discard-only
                      "100::/64",
                      # IETF protocol assignments, including Teredo, benchmarking, and ORCHID
                      "2001::/23",
                      # documentation
                      "2001:db8::/32",
                      # 6to4
                      "2002::/16",
                      # documentation
                      "3fff::/20",
                      # segment routing SIDs
                      "5f00::/16",
                      # unique-local (covers fd00::/8 = AWS IMDS fd00:ec2::254)
                      "fc00::/7",
                      # deprecated site-local
                      "fec0::/10",
                      # link-local
                      "fe80::/10",
                      # multicast
                      "ff00::/8",
                      # outside the currently allocated global-unicast envelope
                      "4000::/2",
                      "8000::/1"
                    ],
                    &InetCidr.parse_cidr!/1
                  )

  @private_ip_error "URL must not target private or reserved IP addresses"

  @doc "Returns true if the address is loopback, link-local, RFC1918, CGNAT, broadcast, or reserved."
  @spec private_ip?(:inet.ip_address()) :: boolean()
  def private_ip?(addr) when is_tuple(addr) do
    Enum.any?(@blocked_ranges, &InetCidr.contains?(&1, addr))
  end

  @doc "Returns only valid public IP addresses, preserving their order."
  @spec public_addresses([term()]) :: [:inet.ip_address()]
  def public_addresses(addresses) when is_list(addresses) do
    addresses
    |> Enum.filter(&public_ip?/1)
    |> Enum.uniq()
  end

  @doc """
  Resolves `host` and returns the first safe IP address, or an error if the
  host has no public address or cannot be resolved at all. Mixed DNS answers
  are filtered so a public address can still be used safely by the pinned TCP
  transport.
  """
  @spec safe_resolve(String.t() | nil) :: {:ok, :inet.ip_address()} | {:error, String.t()}
  def safe_resolve(host) do
    with {:ok, [addr | _]} <- safe_resolve_all(host), do: {:ok, addr}
  end

  @doc """
  Resolves `host` and returns every public IPv4 and IPv6 address, or an error
  if no public address is available.
  """
  @spec safe_resolve_all(String.t() | nil) ::
          {:ok, [:inet.ip_address(), ...]} | {:error, String.t()}
  def safe_resolve_all(host) when is_non_empty_binary(host) do
    charlist = String.to_charlist(host)

    with {:ok, addr} <- :inet.parse_address(charlist),
         {:private, false} <- {:private, private_ip?(addr)} do
      {:ok, [addr]}
    else
      {:error, _} -> resolve_hostname(charlist)
      {:private, true} -> {:error, @private_ip_error}
    end
  end

  def safe_resolve_all(_), do: {:error, "invalid host"}

  defp resolve_hostname(charlist) do
    case TCP.getaddrs(charlist) do
      {:ok, [_ | _] = addresses} -> {:ok, addresses}
      {:error, :eacces} -> {:error, @private_ip_error}
      {:error, _reason} -> {:error, "could not resolve webhook destination host"}
    end
  end

  defp public_ip?(addr), do: :inet.is_ip_address(addr) and not private_ip?(addr)
end
