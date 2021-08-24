defmodule Logflare.Logs.Vector do
  require Logger

  def handle_batch(batch) when is_list(batch) do
    Enum.map(batch, fn x -> handle_event(x) end)
  end

  def handle_event(%{"timestamp" => timestamp, "message" => message} = params) do
    metadata = Map.drop(params, ["message", "timestamp"])

    %{
      "message" => message,
      "metadata" => metadata,
      "timestamp" => timestamp
    }
  end

  def handle_event(%{"timestamp" => timestamp} = params) do
    metadata = Map.drop(params, ["timestamp"])
    message = Jason.encode!(metadata)

    %{
      "message" => message,
      "metadata" => metadata,
      "timestamp" => timestamp
    }
  end
end
