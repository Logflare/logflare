defmodule Logflare.Logs.GenericJson do
  @moduledoc false
  require Logger

  @behaviour Logflare.Logs.Processor

  def handle_batch(batch, _source) when is_list(batch) do
    Enum.map(batch, &handle_event/1)
  end

  def handle_event(params) when is_map(params) do
    %{
      "message" => message(params),
      "metadata" => params
    }
  end

  def message(params) do
    Jason.encode!(params)
  end
end
