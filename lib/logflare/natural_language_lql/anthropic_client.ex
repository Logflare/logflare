defmodule Logflare.NaturalLanguageLql.AnthropicClient do
  @moduledoc false

  @behaviour Logflare.NaturalLanguageLql.Client

  require Logger

  @anthropic_version "2023-06-01"
  @default_base_url "https://api.anthropic.com"
  @default_model "claude-sonnet-5"
  @max_output_tokens 16_000
  @receive_timeout 15_000
  @response_schema %{
    "type" => "object",
    "additionalProperties" => false,
    "properties" => %{
      "kind" => %{"type" => "string", "enum" => ["query", "error"]},
      "lql" => %{"type" => ["string", "null"]},
      "error" => %{
        "anyOf" => [
          %{"type" => "string", "enum" => ["unsupported"]},
          %{"type" => "null"}
        ]
      }
    },
    "required" => ["kind", "lql", "error"]
  }

  @type response :: %{text: String.t(), request_id: String.t() | nil}

  @impl true
  def generate(prompt) when is_binary(prompt) do
    config = config()

    with {:ok, api_key} <- api_key(config),
         {:ok, response} <- complete(api_key, prompt, config) do
      {:ok, response}
    else
      {:error, log_message} when is_binary(log_message) ->
        Logger.error(log_message)
        {:error, :unavailable}
    end
  end

  @spec configured?() :: boolean()
  def configured?, do: match?({:ok, _api_key}, api_key(config()))

  @spec api_key(keyword()) :: {:ok, String.t()} | {:error, String.t()}
  defp api_key(config) do
    case config[:api_key] do
      api_key when is_binary(api_key) ->
        if String.trim(api_key) == "",
          do: {:error, "Anthropic API key is not configured"},
          else: {:ok, api_key}

      _ ->
        {:error, "Anthropic API key is not configured"}
    end
  end

  @spec complete(String.t(), String.t(), keyword()) :: {:ok, response()} | {:error, String.t()}
  defp complete(api_key, prompt, config) do
    model = Keyword.get(config, :model, @default_model)
    endpoint = Keyword.get(config, :base_url, @default_base_url) |> String.trim_trailing("/")

    body =
      Jason.encode!(%{
        model: model,
        messages: [%{role: "user", content: prompt}],
        max_tokens: @max_output_tokens,
        output_config: %{
          effort: "low",
          format: %{type: "json_schema", schema: @response_schema}
        }
      })

    request =
      Finch.build(
        :post,
        endpoint <> "/v1/messages",
        [
          {"anthropic-version", @anthropic_version},
          {"content-type", "application/json"},
          {"x-api-key", api_key}
        ],
        body
      )

    case Finch.request(request, Logflare.FinchDefault, receive_timeout: @receive_timeout) do
      {:ok, %Finch.Response{} = response} ->
        normalize_response(response, Jason.decode(response.body))

      {:error, reason} ->
        {:error, "Anthropic transport failed: #{inspect(reason)}"}
    end
  end

  @spec normalize_response(Finch.Response.t(), {:ok, term()} | {:error, Jason.DecodeError.t()}) ::
          {:ok, response()} | {:error, String.t()}
  defp normalize_response(
         %Finch.Response{status: status} = response,
         {:ok, %{"content" => content}}
       )
       when status in 200..299 and is_list(content) do
    text =
      Enum.find_value(content, fn
        %{"type" => "text", "text" => text} when is_binary(text) and text != "" -> text
        _content -> nil
      end)

    if text do
      {:ok,
       %{
         text: text,
         request_id: response.headers |> Map.new() |> Map.get("request-id")
       }}
    else
      {:error, "Anthropic returned a malformed success response"}
    end
  end

  defp normalize_response(%Finch.Response{status: status}, _decoded) when status in 200..299,
    do: {:error, "Anthropic returned a malformed success response"}

  defp normalize_response(%Finch.Response{status: status, headers: headers}, decoded) do
    body =
      case decoded do
        {:ok, body} when is_map(body) -> body
        _decoded -> %{}
      end

    error = if is_map(body["error"]), do: body["error"], else: %{}
    error_type = if is_binary(error["type"]), do: error["type"], else: "unknown_error"

    error_message =
      if is_binary(error["message"]) do
        error["message"] |> String.replace(~r/[\r\n\t]+/, " ") |> String.slice(0, 500)
      else
        "no provider message"
      end

    request_id = headers |> Map.new() |> Map.get("request-id")
    request_id_suffix = if request_id, do: " (request_id=#{request_id})", else: ""

    {:error,
     "Anthropic request failed with HTTP #{status} (#{error_type}): " <>
       error_message <> request_id_suffix}
  end

  @spec config() :: keyword()
  defp config, do: Application.get_env(:logflare, __MODULE__, [])
end
