defmodule Logflare.Logs.Raw do
  @behaviour Logflare.Logs.Processor

  def handle_batch(data, _source), do: data
end
