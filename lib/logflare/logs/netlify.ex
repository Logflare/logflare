defmodule Logflare.Logs.Netlify do
  @moduledoc """
  See https://elixirforum.com/t/parse-this-string/29252 for a NimbleParsec example.
  """
  require Logger

  def handle_batch(batch, _source) when is_list(batch) do
    Enum.map(batch, &handle_event/1)
  end

  defp handle_event(%{"timestamp" => timestamp, "log_type" => log_type} = event)
       when is_map(event) do
    event = Map.drop(event, ["timestamp"])

    case log_type do
      "traffic" ->
        %{
          "message" => custom_message(event),
          "metadata" => event,
          "timestamp" => timestamp
        }

      "functions" ->
        %{
          "message" => custom_message(event),
          "metadata" => event,
          "timestamp" => timestamp
        }
    end
  end

  defp custom_message(%{
         "log_type" => log_type,
         "method" => method,
         "status_code" => status_code,
         "client_ip" => client_ip,
         "request_id" => request_id,
         "url" => url,
         "user_agent" => user_agent
       }) do
    separator = " | "

    IO.chardata_to_string([
      log_type,
      separator,
      method,
      separator,
      Integer.to_charlist(status_code),
      separator,
      client_ip,
      separator,
      request_id,
      separator,
      url,
      separator,
      user_agent
    ])
  end
end
