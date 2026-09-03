defmodule Logflare.NaturalLanguageLql.AnthropicClientTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import Mimic

  alias Logflare.NaturalLanguageLql.AnthropicClient

  setup :set_mimic_from_context

  setup do
    previous_config = Application.get_env(:logflare, AnthropicClient)

    Application.put_env(:logflare, AnthropicClient,
      api_key: "test-api-key",
      base_url: "https://anthropic.example/",
      model: "claude-sonnet-test"
    )

    on_exit(fn ->
      if previous_config do
        Application.put_env(:logflare, AnthropicClient, previous_config)
      else
        Application.delete_env(:logflare, AnthropicClient)
      end
    end)
  end

  test "sends the prompt and expected response format to Anthropic" do
    prompt = "sensitive-prompt-marker"

    expect(Finch, :request, fn request, Logflare.FinchDefault, receive_timeout: 15_000 ->
      assert request.method == "POST"
      assert request.scheme == :https
      assert request.host == "anthropic.example"
      assert request.path == "/v1/messages"

      headers = Map.new(request.headers)
      assert headers["x-api-key"] == "test-api-key"
      assert headers["anthropic-version"] == "2023-06-01"
      assert headers["content-type"] == "application/json"

      body = Jason.decode!(request.body)
      assert body["model"] == "claude-sonnet-test"
      assert body["messages"] == [%{"role" => "user", "content" => prompt}]

      assert %{
               "type" => "json_schema",
               "schema" => %{
                 "type" => "object",
                 "additionalProperties" => false,
                 "properties" => _properties,
                 "required" => _required
               }
             } = body["output_config"]["format"]

      {:ok,
       %Finch.Response{
         status: 200,
         body: Jason.encode!(%{"content" => [%{"type" => "text", "text" => "generated LQL"}]}),
         headers: [{"request-id", "req_header"}]
       }}
    end)

    assert {:ok, %{text: "generated LQL", request_id: "req_header"}} =
             AnthropicClient.generate(prompt)
  end

  test "logs Anthropic errors and request IDs without exposing sensitive data" do
    response_marker = "sensitive-response-marker"

    expect_response(
      429,
      %{
        "error" => %{
          "type" => "rate_limit_error",
          "message" => "rate\nlimited"
        },
        "marker" => response_marker,
        "request_id" => "req_body"
      },
      [{"request-id", "req_header"}]
    )

    log =
      capture_log(fn ->
        assert {:error, :unavailable} = AnthropicClient.generate("sensitive-prompt-marker")
      end)

    assert log =~ "429"
    assert log =~ "rate_limit_error"
    assert log =~ "rate limited"
    assert log =~ "request_id=req_header"
    refute log =~ "req_body"
    refute log =~ "sensitive-prompt-marker"
    refute log =~ "test-api-key"
    refute log =~ response_marker
  end

  test "returns the first text response and ignores IDs in the response body" do
    expect_response(200, %{
      "id" => "msg_body",
      "request_id" => "req_body",
      "content" => [
        %{"type" => "tool_use", "id" => "tool_123"},
        %{"type" => "text", "text" => ""},
        %{"type" => "text", "text" => "generated LQL"}
      ]
    })

    assert {:ok, %{text: "generated LQL", request_id: nil}} =
             AnthropicClient.generate("find errors")
  end

  test "does not send a request without an API key" do
    Application.put_env(:logflare, AnthropicClient,
      api_key: "  ",
      base_url: "https://anthropic.example"
    )

    refute AnthropicClient.configured?()
    reject(Finch, :request, 3)

    assert capture_log(fn ->
             assert {:error, :unavailable} = AnthropicClient.generate("sensitive-prompt-marker")
           end) =~ "API key is not configured"
  end

  defp expect_response(status, body, headers \\ []) do
    body = if is_binary(body), do: body, else: Jason.encode!(body)

    expect(Finch, :request, fn _request, _pool, _options ->
      {:ok, %Finch.Response{status: status, body: body, headers: headers}}
    end)
  end
end
