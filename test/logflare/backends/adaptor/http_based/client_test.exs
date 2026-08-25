defmodule Logflare.Backends.Adaptor.HttpBased.ClientTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.HttpBased.Client
  alias Logflare.Tesla.MockAdapter

  defmodule ContentTypeFormatter do
    @behaviour Tesla.Middleware

    def reserved_headers, do: ["content-type"]

    @impl true
    def call(env, next, _opts) do
      env
      |> Tesla.put_header("content-type", "application/vnd.logflare.test")
      |> Tesla.run(next)
    end
  end

  test "formatter-owned headers replace user-supplied copies" do
    client =
      [
        headers: [{"Content-Type", "text/plain"}, {"X-Custom", "kept"}],
        formatter: ContentTypeFormatter,
        json: false
      ]
      |> Client.new()
      |> MockAdapter.replace(fn env -> {:ok, %{env | status: 204}} end)

    assert {:ok, env} = Tesla.post(client, "https://example.com", "payload")

    assert Enum.filter(env.headers, fn {name, _value} ->
             String.downcase(name) == "content-type"
           end) == [{"content-type", "application/vnd.logflare.test"}]

    assert {"X-Custom", "kept"} in env.headers
  end

  test "SSRF protection selects the safe Finch pool by default and ignores overrides" do
    for ssrf_opts <- [[], [ssrf: true]], http2? <- [true, false] do
      client =
        Client.new(ssrf_opts ++ [pool_name: Logflare.UnsafeFinch, http2: http2?])

      assert {Tesla.Adapter.Finch, [name: Logflare.FinchSSRF, receive_timeout: 5_000]} =
               Tesla.Client.adapter(client)
    end
  end

  test "explicitly disabling SSRF protection uses ordinary Finch pools" do
    for {opts, pool_name} <- [
          {[ssrf: false], Logflare.FinchDefault},
          {[ssrf: false, http2: false], Logflare.FinchDefaultHttp1},
          {[ssrf: false, pool_name: Logflare.TrustedFinch], Logflare.TrustedFinch}
        ] do
      assert {Tesla.Adapter.Finch, [name: ^pool_name, receive_timeout: 5_000]} =
               opts
               |> Client.new()
               |> Tesla.Client.adapter()
    end
  end
end
