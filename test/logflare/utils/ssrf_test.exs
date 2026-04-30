defmodule Logflare.Utils.SSRFTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Logflare.Utils.SSRF

  describe "private_ip?/1" do
    test "blocks loopback, RFC1918, link-local, CGNAT, broadcast IPv4" do
      blocked = [
        {127, 0, 0, 1},
        {127, 255, 255, 255},
        {10, 0, 0, 1},
        {10, 255, 255, 255},
        {172, 16, 0, 1},
        {172, 31, 255, 255},
        {192, 168, 0, 1},
        {169, 254, 169, 254},
        {0, 0, 0, 0},
        {100, 64, 0, 1},
        {100, 127, 255, 255},
        {255, 255, 255, 255}
      ]

      for addr <- blocked do
        assert SSRF.private_ip?(addr), "expected private for #{inspect(addr)}"
      end
    end

    test "allows public IPv4 addresses" do
      for addr <- [{1, 1, 1, 1}, {8, 8, 8, 8}, {172, 15, 0, 1}, {172, 32, 0, 1}] do
        refute SSRF.private_ip?(addr), "expected public for #{inspect(addr)}"
      end
    end

    test "blocks private IPv6 addresses" do
      blocked = [
        # loopback ::1
        {0, 0, 0, 0, 0, 0, 0, 1},
        # unspecified ::
        {0, 0, 0, 0, 0, 0, 0, 0},
        # link-local fe80::1
        {0xFE80, 0, 0, 0, 0, 0, 0, 1},
        # unique local fc00::1, fd00::1
        {0xFC00, 0, 0, 0, 0, 0, 0, 1},
        {0xFD00, 0, 0, 0, 0, 0, 0, 1},
        # IPv4-mapped ::ffff:192.168.1.1
        {0, 0, 0, 0, 0, 0xFFFF, 0xC0A8, 0x0101},
        # AWS IMDS IPv6 fd00:ec2::254
        {0xFD00, 0x0EC2, 0, 0, 0, 0, 0, 0x00FE}
      ]

      for addr <- blocked do
        assert SSRF.private_ip?(addr), "expected private for #{inspect(addr)}"
      end
    end

    test "allows public IPv6 addresses" do
      # 2001:4860:4860::8888 (Google DNS)
      refute SSRF.private_ip?({0x2001, 0x4860, 0x4860, 0, 0, 0, 0, 0x8888})
    end
  end

  describe "safe_resolve/1" do
    test "returns error for nil host" do
      assert {:error, _} = SSRF.safe_resolve(nil)
    end

    test "returns error for literal private IPv4" do
      assert {:error, _} = SSRF.safe_resolve("127.0.0.1")
      assert {:error, _} = SSRF.safe_resolve("192.168.1.1")
      assert {:error, _} = SSRF.safe_resolve("169.254.169.254")
    end

    test "returns ok with address tuple for literal public IPv4" do
      assert {:ok, {1, 1, 1, 1}} = SSRF.safe_resolve("1.1.1.1")
      assert {:ok, {8, 8, 8, 8}} = SSRF.safe_resolve("8.8.8.8")
    end

    test "returns error for literal private IPv6" do
      assert {:error, _} = SSRF.safe_resolve("::1")
      assert {:error, _} = SSRF.safe_resolve("fe80::1")
      assert {:error, _} = SSRF.safe_resolve("fc00::1")
    end

    test "returns error for hostname resolving to loopback" do
      assert {:error, _} = SSRF.safe_resolve("localhost")
    end
  end

  describe "url_host/1" do
    test "formats IPv4 as plain string" do
      assert SSRF.url_host({1, 2, 3, 4}) == "1.2.3.4"
    end

    test "formats IPv6 with brackets" do
      assert SSRF.url_host({0, 0, 0, 0, 0, 0, 0, 1}) == "[::1]"
      assert SSRF.url_host({0x2001, 0x4860, 0x4860, 0, 0, 0, 0, 0x8888}) ==
               "[2001:4860:4860::8888]"
    end
  end
end
