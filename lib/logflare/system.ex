defmodule Logflare.System do
  @moduledoc """
  APIs for retrieving system information across all features and underlying OS.

  Intended to be used for health checks and monitoring.
  """

  @doc """
  Retrieve memory utilization ratio based on system availble memory from :memsup, against total allocated.

  iex> is_float(memory_utilization())
  true
  """
  @spec memory_utilization() :: float()
  def memory_utilization() do
    data = :memsup.get_system_memory_data()
    total = data[:system_total_memory]
    used = :erlang.memory(:total)

    if is_nil(total) do
      # memsup not available
      0.0
    else
      Float.round(used / total, 8)
    end
  end
end
