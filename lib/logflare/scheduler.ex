defmodule Logflare.Scheduler do
  @moduledoc """
  Quantum scheduler for periodic tasks.
  """
  use Quantum, otp_app: :logflare, restart: :transient

  @doc """
  Returns the scheduler :via name used for syn registry.
  """
  def scheduler_name do
    ts = DateTime.utc_now() |> DateTime.to_unix(:nanosecond)
    # add nanosecond resolution for timestamp comparison
    {:via, :syn, {:scheduler, __MODULE__, %{timestamp: ts}}}
  end
end
