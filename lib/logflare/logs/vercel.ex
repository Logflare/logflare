defmodule Logflare.Logs.Vercel do
  @moduledoc """
  See https://elixirforum.com/t/parse-this-string/29252 for a NimbleParsec example.
  """
  require Logger

  alias Logflare.Logs.Vercel.NimbleLambdaMessageParser

  def handle_batch(batch, source) when is_list(batch) do
    Enum.map(batch, fn x ->
      if (x["source"] == "lambda" or x["source"] == "edge") and x["message"] do
        {_status, lambda_message} = try_lambda_parse(x["message"], source)

        Map.put(x, "parsedLambdaMessage", lambda_message)
        |> handle_event()
      else
        handle_event(x)
      end
    end)
  end

  defp try_lambda_parse(message, source) do
    try do
      NimbleLambdaMessageParser.parse(message)
    rescue
      _e ->
        Logger.error("Lambda parse error!",
          source_id: source.token,
          vercel_app: %{lambda_message: message, parse_status: "error"}
        )

        {:error, %{"parse_status" => "error"}}
    end
  end

  defp handle_event(event) when is_map(event) do
    source = event["source"]

    case source do
      "build" ->
        message = event["message"]

        %{
          "message" => "#{source} | " <> message,
          "metadata" => user_agent_to_string(event),
          "timestamp" => event["timestamp"]
        }

      _else ->
        message = custom_message(event)

        %{
          "message" => "#{source} | " <> message,
          "metadata" => user_agent_to_string(event),
          "timestamp" => event["timestamp"]
        }
    end
  end

  defp user_agent_to_string(event) when is_map(event) do
    if event["proxy"]["userAgent"] do
      ua =
        event["proxy"]["userAgent"]
        |> List.wrap()
        |> Enum.join(",")

      put_in(event["proxy"]["userAgent"], ua)
    else
      event
    end
  end

  defp custom_message(event) do
    "#{event["proxy"]["statusCode"]} | #{event["proxy"]["host"]} | #{event["proxy"]["path"]} | #{
      event["proxy"]["clientIp"]
    } | #{event["proxy"]["userAgent"]}"
  end
end
