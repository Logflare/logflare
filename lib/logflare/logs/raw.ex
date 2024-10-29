defmodule Logflare.Logs.Raw do
  @behaviour Logflare.Logs.Processor

  @spec handle_batch(maybe_improper_list(), any()) :: list()
  def handle_batch(data, _source) when is_list(data) do
    for event <- data do
      handle_event(event)
    end
  end

  @spec handle_event(map()) :: map()
  def handle_event(%{"message" => _messsage} = event) do
    event
  end

  def handle_event(%{"event_message" => _messsage} = event) do
    event
  end

  def handle_event(event) do
    case Jason.encode(event) do
      {:ok, json} -> Map.put(event, "message", json)
      {:error, _error} -> Map.put(event, "message", "JSONDecodeError")
    end
  end
end
