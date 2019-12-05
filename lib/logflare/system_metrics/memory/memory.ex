defmodule Logflare.SystemMetrics.Memory do
  def get_memory() do
    :erlang.memory() |> Enum.map(fn {k, v} -> {k, div(v, 1024 * 1024)} end) |> Enum.into(%{})
  end
end
