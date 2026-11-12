defmodule Logflare.Utils.SSRFTest do
  @moduledoc false
  use ExUnit.Case, async: true
  use Mimic

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

    test "returns error for hostname with public IPv4 and private IPv6 records" do
      for private_ipv6 <- [
            {0, 0, 0, 0, 0, 0, 0, 1},
            {0xFC00, 0, 0, 0, 0, 0, 0, 1},
            {0xFE80, 0, 0, 0, 0, 0, 0, 1}
          ] do
        stub_dns(inet: {:ok, [{8, 8, 8, 8}]}, inet6: {:ok, [private_ipv6]})

        assert {:error, "URL must not target private or reserved IP addresses"} =
                 SSRF.safe_resolve("rebind.example.com")
      end
    end

    test "returns error for hostname with an IPv4-mapped private IPv6 record" do
      stub_dns(inet: {:ok, [{8, 8, 8, 8}]}, inet6: {:ok, [{0, 0, 0, 0, 0, 0xFFFF, 0x7F00, 1}]})

      assert {:error, _} = SSRF.safe_resolve("mapped.example.com")
    end

    test "returns the IPv4 address for hostname with public IPv4 and IPv6 records" do
      stub_dns(
        inet: {:ok, [{8, 8, 8, 8}]},
        inet6: {:ok, [{0x2001, 0x4860, 0x4860, 0, 0, 0, 0, 0x8888}]}
      )

      assert {:ok, {8, 8, 8, 8}} = SSRF.safe_resolve("dual.example.com")
    end

    test "returns the IPv6 address for hostname with only public IPv6 records" do
      ipv6 = {0x2001, 0x4860, 0x4860, 0, 0, 0, 0, 0x8888}
      stub_dns(inet: {:error, :nxdomain}, inet6: {:ok, [ipv6]})

      assert {:ok, ^ipv6} = SSRF.safe_resolve("v6only.example.com")
    end

    test "returns error for hostname without any records" do
      stub_dns(inet: {:ok, []}, inet6: {:error, :nxdomain})

      assert {:error, "could not resolve webhook destination host"} =
               SSRF.safe_resolve("missing.example.com")
    end
  end

  describe "validate_url_host/2" do
    test "accepts a url with a public host" do
      assert %Ecto.Changeset{valid?: true} =
               url_changeset("https://8.8.8.8/path") |> SSRF.validate_url_host(:url)
    end

    test "rejects a url with a private or reserved host" do
      changeset = url_changeset("https://169.254.169.254/") |> SSRF.validate_url_host(:url)

      assert [url: {"URL must not target private or reserved IP addresses", [validation: :ssrf]}] =
               changeset.errors
    end

    test "skips a missing url" do
      assert %Ecto.Changeset{valid?: true} =
               url_changeset(nil) |> SSRF.validate_url_host(:url)
    end

    test "skips a url that already has an error" do
      changeset =
        url_changeset("https://127.0.0.1")
        |> Ecto.Changeset.add_error(:url, "has invalid format")
        |> SSRF.validate_url_host(:url)

      assert [url: {"has invalid format", []}] = changeset.errors
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

  defp stub_dns(answers) do
    stub(SSRF, :getaddrs, fn _charlist, family -> Keyword.fetch!(answers, family) end)
  end

  defp url_changeset(url) do
    Ecto.Changeset.cast({%{}, %{url: :string}}, %{url: url}, [:url])
  end
end
