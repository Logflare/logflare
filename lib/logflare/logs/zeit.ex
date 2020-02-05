defmodule Logflare.Logs.Zeit do
  require Logger

  def handle_batch(batch, source) when is_list(batch) do
    Enum.map(batch, fn x ->
      case x["source"] do
        "lambda" ->
          lambda_message =
            try do
              parse_lambda_message(x["message"])
            rescue
              _e ->
                Logger.error("Lambda parse error!", source_id: source.id)

                %{"parse_status" => "error"}
            end

          Map.put(x, "parsedLambdaMessage", lambda_message)
          |> handle_event()

        _ ->
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
      [ua] = event["proxy"]["userAgent"]
      Kernel.put_in(event["proxy"]["userAgent"], ua)
    else
      event
    end
  end

  defp custom_message(event) do
    "#{event["proxy"]["statusCode"]} | #{event["proxy"]["host"]} | #{event["proxy"]["path"]} | #{
      event["proxy"]["clientIp"]
    } | #{event["proxy"]["userAgent"]}"
  end

  defp parse_lambda_message(message) do
    String.split(message, "\n")
    |> Enum.find(fn x -> String.contains?(x, "REPORT") end)
    |> String.split("\t")
    |> Enum.drop_while(fn x -> String.contains?(x, "RequestId:") == true end)
    |> Enum.map(fn x ->
      case String.split(x, ":", trim: true) do
        [k, v] ->
          [v, kind] =
            String.trim(v)
            |> String.split(" ")

          key =
            "#{k}_#{kind}"
            |> String.downcase()
            |> String.replace(" ", "_")

          value =
            case Integer.parse(v) do
              {int, _rem} -> int
              _ -> v
            end

          {key, value}

        _ ->
          {"key", "value"}
      end
    end)
    |> Enum.into(%{})
    |> Map.drop(["key"])
    |> Map.put("parse_status", "success")
  end
end
