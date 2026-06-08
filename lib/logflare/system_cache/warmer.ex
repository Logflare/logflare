defmodule Logflare.SystemCache.Warmer do
  @moduledoc false

  use Cachex.Warmer

  @impl true
  def execute(_state) do
    {:ok, [{:memory_utilization, Logflare.System.memory_utilization()}]}
  end
end
