defmodule Logflare.Backends.Adaptor.S3Adaptor.HttpClientTest do
  use ExUnit.Case, async: false
  use Mimic

  alias Logflare.Backends.Adaptor.S3Adaptor.HttpClient

  setup :set_mimic_global
  setup :verify_on_exit!

  test "sends requests through the dedicated Finch pool with the configured deadlines" do
    http_opts = [pool_timeout: 5_000, receive_timeout: 30_000, request_timeout: 60_000]

    Finch
    |> expect(:build, fn :put, "https://example.com/object", headers, "body" ->
      assert headers == [{"content-type", "application/octet-stream"}]
      :request
    end)
    |> expect(:request, fn :request, Logflare.FinchS3, ^http_opts ->
      {:ok, %Finch.Response{status: 200, headers: [{"etag", "abc"}], body: "ok"}}
    end)

    assert {:ok,
            %{
              status_code: 200,
              headers: [{"etag", "abc"}],
              body: "ok"
            }} =
             HttpClient.request(
               :put,
               "https://example.com/object",
               "body",
               [{"content-type", "application/octet-stream"}],
               http_opts
             )
  end

  test "normalizes Finch transport failures for ExAws" do
    Finch
    |> expect(:build, fn :put, "https://example.com/object", [], "body" -> :request end)
    |> expect(:request, fn :request, Logflare.FinchS3, [] ->
      {:error, %Mint.TransportError{reason: :timeout}}
    end)

    assert {:error, %{reason: %Mint.TransportError{reason: :timeout}}} =
             HttpClient.request(:put, "https://example.com/object", "body", [], [])
  end
end
