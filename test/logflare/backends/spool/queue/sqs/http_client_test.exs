defmodule Logflare.Backends.Spool.Queue.SQS.HttpClientTest do
  use ExUnit.Case, async: false
  use Mimic

  alias Logflare.Backends.Spool.Queue.SQS.HttpClient

  setup :set_mimic_global
  setup :verify_on_exit!

  test "sends requests through SQS's own dedicated Finch pool" do
    http_opts = [pool_timeout: 5_000, receive_timeout: 30_000, request_timeout: 60_000]

    Finch
    |> expect(:build, fn :post, "https://example.com/queue", headers, "body" ->
      assert headers == [{"content-type", "application/x-www-form-urlencoded"}]
      :request
    end)
    |> expect(:request, fn :request, Logflare.FinchSpoolSQS, ^http_opts ->
      {:ok, %Finch.Response{status: 200, headers: [{"x-amzn-requestid", "abc"}], body: "ok"}}
    end)

    assert {:ok,
            %{
              status_code: 200,
              headers: [{"x-amzn-requestid", "abc"}],
              body: "ok"
            }} =
             HttpClient.request(
               :post,
               "https://example.com/queue",
               "body",
               [{"content-type", "application/x-www-form-urlencoded"}],
               http_opts
             )
  end

  test "normalizes Finch transport failures for ExAws" do
    Finch
    |> expect(:build, fn :post, "https://example.com/queue", [], "body" -> :request end)
    |> expect(:request, fn :request, Logflare.FinchSpoolSQS, [] ->
      {:error, %Mint.TransportError{reason: :timeout}}
    end)

    assert {:error, %{reason: %Mint.TransportError{reason: :timeout}}} =
             HttpClient.request(:post, "https://example.com/queue", "body", [], [])
  end

  test "normalizes Finch HTTP/1 pool checkout timeouts for ExAws" do
    Finch
    |> expect(:build, fn :post, "https://example.com/queue", [], "body" -> :request end)
    |> expect(:request, fn :request, Logflare.FinchSpoolSQS, [] ->
      raise "Finch was unable to provide a connection within the timeout due to excess queuing"
    end)

    assert {:error, %{reason: :pool_timeout}} =
             HttpClient.request(:post, "https://example.com/queue", "body", [], [])
  end

  test "re-raises unrelated Finch runtime failures" do
    Finch
    |> expect(:build, fn :post, "https://example.com/queue", [], "body" -> :request end)
    |> expect(:request, fn :request, Logflare.FinchSpoolSQS, [] -> raise "unrelated boom" end)

    assert_raise RuntimeError, "unrelated boom", fn ->
      HttpClient.request(:post, "https://example.com/queue", "body", [], [])
    end
  end
end
