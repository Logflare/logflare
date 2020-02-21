defmodule Logflare.Logs.Zeit do
  @moduledoc """
  See https://elixirforum.com/t/parse-this-string/29252 for a NimbleParsec example.
  """
  require Logger

  alias Logflare.Logs.Zeit.LambdaMessageParser

  def handle_batch(batch, source) when is_list(batch) do
    Enum.map(batch, fn x ->
      if x["source"] == "lambda" and x["message"] do
        {:ok, lambda_message} =
          try do
            LambdaMessageParser.parse(x["message"])
          rescue
            _e ->
              Logger.error("Lambda parse error!",
                source_id: source.token,
                zeit_app: %{lambda_message: x["message"], parse_status: "error"}
              )

              %{"parse_status" => "error"}
          end

        Map.put(x, "parsedLambdaMessage", lambda_message)
        |> handle_event()
      else
        handle_event(x)
      end
    end)
  end

  defp handle_event(event) when is_map(event) do
    source = event["source"]
    message = event["message"] || custom_message(event)

    %{
      "message" => "#{source} | " <> message,
      "metadata" => user_agent_to_string(event)
    }
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
