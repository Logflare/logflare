defmodule Logflare.Scheduler do
  @moduledoc """
  Quantum scheduler for periodic tasks.
  """
  use Quantum, otp_app: :logflare, restart: :transient

  @doc """
  Returns the scheduler :via name used for syn registry.
  """
  def scheduler_name do
    ts = System.os_time(:nanosecond)
    # add nanosecond resolution for timestamp comparison
    {:via, :syn, {:core, __MODULE__, %{timestamp: ts}}}
  end
end
