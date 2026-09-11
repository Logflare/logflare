defmodule Logflare.SystemCache.Warmer do
  @moduledoc false

  use Cachex.Warmer

  require Logger

  @impl true
  def execute(_state) do
    {:ok, [{:memory_utilization, Logflare.System.memory_utilization()}]}
  rescue
    error ->
      Logger.warning("SystemCache warmer failed: #{Exception.message(error)}")
      :ignore
  catch
    kind, reason ->
      Logger.warning(
        "SystemCache warmer failed: #{Exception.format(kind, reason, __STACKTRACE__)}"
      )

      :ignore
  end
end
