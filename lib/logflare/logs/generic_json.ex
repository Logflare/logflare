defmodule Logflare.Logs.GenericJson do
  require Logger

  def handle_batch(batch) when is_list(batch) do
    Enum.map(batch, fn x -> handle_event(x) end)
  end

  def handle_event(params) when is_map(params) do
    %{
      "message" => message(params),
      "metadata" => params
    }
  end

  def message(params) do
    inspect(params)
  end
end
