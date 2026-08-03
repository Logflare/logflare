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
  def memory_utilization do
    used = :erlang.memory(:total)

    case total_memory_bytes() do
      nil -> 0.0
      total -> Float.round(used / total, 8)
    end
  end

  @doc """
  Total system memory in bytes, from :memsup. Returns `nil` if memsup is unavailable.

  iex> is_integer(total_memory_bytes()) or is_nil(total_memory_bytes())
  true
  """
  @spec total_memory_bytes() :: non_neg_integer() | nil
  def total_memory_bytes do
    :memsup.get_system_memory_data()[:system_total_memory]
  end
end
