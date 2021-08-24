defmodule Logflare.Logs.Vector do
  require Logger

  def handle_batch(batch) when is_list(batch) do
    Enum.map(batch, fn x -> handle_event(x) end)
  end

  def handle_event(%{"timestamp" => timestamp, "message" => message} = params) do
    metadata =
      case params do
        %{"log" => %{"level" => level}} ->
          Map.put(params, "level", level)

        _ ->
          params
      end
      |> Map.drop(["message", "timestamp"])

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
