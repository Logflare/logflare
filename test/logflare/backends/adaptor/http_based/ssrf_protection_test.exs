defmodule Logflare.Backends.Adaptor.HttpBased.SSRFProtectionTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.HttpBased.SSRFProtection

  defp ok_next(env), do: {:ok, env}
  defp call(url), do: SSRFProtection.call(%Tesla.Env{url: url, headers: []}, &ok_next/1, [])

  describe "call/3 with HTTP URLs" do
    test "blocks private IPv4 at request time" do
      for url <- [
            "http://127.0.0.1/",
            "http://10.0.0.1/",
            "http://192.168.1.1/",
            "http://169.254.169.254/latest/meta-data/"
          ] do
        assert {:error, _reason} = call(url), "expected block for #{url}"
      end
    end

    test "blocks private IPv6 at request time" do
      assert {:error, _} = call("http://[::1]/")
      assert {:error, _} = call("http://[fc00::1]/")
    end

    test "rewrites HTTP URL to resolved IP and preserves Host header" do
      {:ok, env} = call("http://127.0.0.1/")
      # Only reaches here for public IPs — use a public literal to verify rewrite
      # (127.0.0.1 is blocked; test rewrite with a non-blocked literal)
      {:ok, env} = SSRFProtection.call(
        %Tesla.Env{url: "http://1.2.3.4/path", headers: []},
        &ok_next/1,
        []
      )

      assert env.url == "http://1.2.3.4/path"
      assert {"host", "1.2.3.4"} in env.headers
    end
  end

  describe "call/3 with HTTPS URLs" do
    test "blocks private IPv4 at request time" do
      assert {:error, _} = call("https://127.0.0.1/")
      assert {:error, _} = call("https://169.254.169.254/")
    end

    test "does not rewrite URL for HTTPS (preserves TLS SNI)" do
      {:ok, env} = SSRFProtection.call(
        %Tesla.Env{url: "https://1.2.3.4/path", headers: []},
        &ok_next/1,
        []
      )

      assert env.url == "https://1.2.3.4/path"
      refute List.keymember?(env.headers, "host", 0)
    end
  end
end
