defmodule Logflare.Backends.Adaptor.HttpBased.SSRFProtectionTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.HttpBased.SSRFProtection

  defp ok_next(env), do: {:ok, env}

  defp call(url, opts \\ []) do
    env = struct!(Tesla.Env, Keyword.put(opts, :url, url))
    SSRFProtection.call(env, [{:fn, &ok_next/1}], [])
  end

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
      {:ok, env} = call("http://1.2.3.4/path")

      assert env.url == "http://1.2.3.4/path"
      assert {"host", "1.2.3.4"} in env.headers
    end

    test "replaces all existing Host headers case-insensitively" do
      {:ok, env} =
        call("http://1.2.3.4/path",
          headers: [
            {"Host", "other.example"},
            {"hOsT", "duplicate.example"},
            {"host", "third.example"},
            {"authorization", "Bearer token"}
          ]
        )

      host_headers =
        Enum.filter(env.headers, fn {key, _value} -> String.downcase(key) == "host" end)

      assert host_headers == [{"host", "1.2.3.4"}]
      assert {"authorization", "Bearer token"} in env.headers
    end
  end

  describe "call/3 with HTTPS URLs" do
    test "blocks private IPv4 at request time" do
      assert {:error, _} = call("https://127.0.0.1/")
      assert {:error, _} = call("https://169.254.169.254/")
    end

    test "does not rewrite URL for HTTPS (preserves TLS SNI)" do
      {:ok, env} =
        SSRFProtection.call(
          %Tesla.Env{url: "https://1.2.3.4/path", headers: []},
          [{:fn, &ok_next/1}],
          []
        )

      assert env.url == "https://1.2.3.4/path"
      refute List.keymember?(env.headers, "host", 0)
    end
  end
end
