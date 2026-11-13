defmodule Logflare.Utils.SSRFTest do
  use ExUnit.Case, async: false

  alias Logflare.Utils.SSRF

  @public_ipv4 {1, 1, 1, 1}
  @public_ipv6 {0x2606, 0x4700, 0x4700, 0, 0, 0, 0, 0x1111}

  describe "private_ip?/1" do
    test "blocks private and special-use IPv4 addresses" do
      blocked = [
        {0, 0, 0, 0},
        {10, 0, 0, 1},
        {100, 64, 0, 1},
        {100, 127, 255, 255},
        {127, 0, 0, 1},
        {127, 255, 255, 255},
        {169, 254, 169, 254},
        {172, 16, 0, 1},
        {172, 31, 255, 255},
        {192, 0, 0, 1},
        {192, 0, 2, 1},
        {192, 88, 99, 1},
        {192, 168, 0, 1},
        {198, 18, 0, 1},
        {198, 51, 100, 1},
        {203, 0, 113, 1},
        {224, 0, 0, 1},
        {240, 0, 0, 1},
        {255, 255, 255, 255}
      ]

      for address <- blocked do
        assert SSRF.private_ip?(address), "expected blocked for #{inspect(address)}"
      end
    end

    test "allows public IPv4 addresses and private-range boundaries" do
      for address <- [@public_ipv4, {8, 8, 8, 8}, {172, 15, 0, 1}, {172, 32, 0, 1}] do
        refute SSRF.private_ip?(address), "expected public for #{inspect(address)}"
      end
    end

    test "blocks private and special-use IPv6 addresses" do
      blocked = [
        {0, 0, 0, 0, 0, 0, 0, 0},
        {0, 0, 0, 0, 0, 0, 0, 1},
        {0, 0, 0, 0, 0, 0xFFFF, 0xC0A8, 0x0101},
        {0x0064, 0xFF9B, 0, 0, 0, 0, 0, 1},
        {0x0064, 0xFF9B, 1, 0, 0, 0, 0, 1},
        {0x0100, 0, 0, 0, 0, 0, 0, 1},
        {0x1800, 0, 0, 0, 0, 0, 0, 1},
        {0x2001, 0, 0, 0, 0, 0, 0, 1},
        {0x2001, 2, 0, 0, 0, 0, 0, 1},
        {0x2001, 5, 0, 0, 0, 0, 0, 1},
        {0x2001, 0x0DB8, 0, 0, 0, 0, 0, 1},
        {0x2002, 0, 0, 0, 0, 0, 0, 1},
        {0x3FFF, 0, 0, 0, 0, 0, 0, 1},
        {0x4000, 0, 0, 0, 0, 0, 0, 1},
        {0x5F00, 0, 0, 0, 0, 0, 0, 1},
        {0xFC00, 0, 0, 0, 0, 0, 0, 1},
        {0xFD00, 0x0EC2, 0, 0, 0, 0, 0, 0x00FE},
        {0xFEC0, 0, 0, 0, 0, 0, 0, 1},
        {0xFE80, 0, 0, 0, 0, 0, 0, 1},
        {0xFF00, 0, 0, 0, 0, 0, 0, 1}
      ]

      for address <- blocked do
        assert SSRF.private_ip?(address), "expected blocked for #{inspect(address)}"
      end
    end

    test "allows public IPv6 addresses" do
      refute SSRF.private_ip?(@public_ipv6)
      refute SSRF.private_ip?({0x2001, 0x4860, 0x4860, 0, 0, 0, 0, 0x8888})
    end
  end

  describe "public_addresses/1" do
    test "filters blocked and invalid entries while preserving and deduplicating public addresses" do
      addresses = [
        @public_ipv4,
        {127, 0, 0, 1},
        @public_ipv6,
        :invalid,
        {999, 0, 0, 1},
        @public_ipv4
      ]

      assert SSRF.public_addresses(addresses) == [@public_ipv4, @public_ipv6]
    end
  end

  describe "safe_resolve/1 and safe_resolve_all/1" do
    test "reject invalid hosts" do
      assert {:error, "invalid host"} = SSRF.safe_resolve(nil)
      assert {:error, "invalid host"} = SSRF.safe_resolve_all("")
    end

    test "reject private literal addresses" do
      for host <- [
            "127.0.0.1",
            "192.168.1.1",
            "169.254.169.254",
            "::1",
            "fe80::1",
            "fc00::1"
          ] do
        assert {:error, reason} = SSRF.safe_resolve_all(host)
        assert reason =~ "private or reserved"
      end
    end

    test "returns public literal addresses" do
      assert {:ok, [@public_ipv4]} = SSRF.safe_resolve_all("1.1.1.1")
      assert {:ok, @public_ipv4} = SSRF.safe_resolve("1.1.1.1")

      assert {:ok, [@public_ipv6]} =
               SSRF.safe_resolve_all("2606:4700:4700::1111")
    end

    test "returns all public records while filtering private records from a mixed answer" do
      host = "ssrf-mixed.test"
      second_public_ipv4 = {8, 8, 8, 8}
      second_public_ipv6 = {0x2001, 0x4860, 0x4860, 0, 0, 0, 0, 0x8888}

      put_host_addresses(host, [
        {127, 0, 0, 1},
        @public_ipv4,
        second_public_ipv4,
        @public_ipv6,
        {0, 0, 0, 0, 0, 0, 0, 1},
        second_public_ipv6
      ])

      expected = [@public_ipv4, @public_ipv6, second_public_ipv4, second_public_ipv6]

      assert {:ok, ^expected} = SSRF.safe_resolve_all(host)
      assert {:ok, @public_ipv4} = SSRF.safe_resolve(host)
    end

    test "returns the private-address error when every resolved record is blocked" do
      host = "ssrf-private.test"
      put_host_addresses(host, [{127, 0, 0, 1}, {0, 0, 0, 0, 0, 0, 0, 1}])

      assert {:error, reason} = SSRF.safe_resolve_all(host)
      assert reason =~ "private or reserved"
    end

    test "returns the resolution error when neither address family resolves" do
      use_file_lookup()

      assert {:error, "could not resolve webhook destination host"} =
               SSRF.safe_resolve_all("ssrf-unresolved.test")
    end

    test "rejects localhost" do
      assert {:error, _reason} = SSRF.safe_resolve("localhost")
    end
  end

  defp put_host_addresses(host, addresses) do
    use_file_lookup()
    hostname = String.to_charlist(host)

    Enum.each(addresses, &(:ok = :inet_db.add_host(&1, [hostname])))

    on_exit(fn ->
      Enum.each(addresses, &(:ok = :inet_db.del_host(&1)))
    end)
  end

  defp use_file_lookup do
    previous_lookup = :inet_db.res_option(:lookup)
    :ok = :inet_db.set_lookup([:file])
    on_exit(fn -> :ok = :inet_db.set_lookup(previous_lookup) end)
  end
end
