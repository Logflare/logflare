defmodule Logflare.SystemCache.Warmer do
  @moduledoc false

  use Cachex.Warmer

  require Logger

  @impl true
  def execute(_state) do
    {:ok, [{:memory_utilization, Logflare.System.memory_utilization()}]}
  rescue
    e ->
      Logger.warning("SystemCache warmer failed: #{inspect(e)}")
      :ignore
  end
end
