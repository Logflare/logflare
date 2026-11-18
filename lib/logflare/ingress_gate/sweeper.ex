defmodule Logflare.IngressGate.Sweeper do
  @moduledoc false

  use Task, restart: :permanent

  alias Logflare.IngressGate

  @interval_ms 10_000

  @spec start_link(keyword()) :: {:ok, pid()}
  def start_link(opts) do
    interval = Keyword.get(opts, :interval_ms, @interval_ms)
    Task.start_link(fn -> sweep(interval) end)
  end

  defp sweep(interval) do
    Process.sleep(interval)
    IngressGate.sweep_dead_leases()
    sweep(interval)
  end
end
