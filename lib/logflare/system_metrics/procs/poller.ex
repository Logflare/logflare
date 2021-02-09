defmodule Logflare.SystemMetrics.Procs.Poller do
  @moduledoc """
  Polls process for process stats.

  Process calculations inspired by: https://github.com/sasa1977/demo_system/blob/master/example_system/lib/runtime.ex
  """

  use GenServer

  alias Logflare.SystemMetrics.Wobserver

  require Logger

  @poll_every 30_000

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  def init(state) do
    poll_metrics(Enum.random(0..:timer.seconds(60)))
    {:ok, state}
  end

  def handle_info(:poll_metrics, state) do
    procs =
      Wobserver.Processes.list()
      |> Stream.reject(&(&1 == :error))

    reds_sorted =
      procs
      |> Enum.sort_by(& &1.reductions, :desc)
      |> Stream.take(10)
      |> Enum.to_list()

    mem_sorted =
      procs
      |> Enum.sort_by(& &1.memory, :desc)
      |> Stream.take(10)
      |> Enum.to_list()

    processes = mem_sorted ++ reds_sorted

    if Application.get_env(:logflare, :env) == :prod do
      Logger.info("Process metrics!", processes: processes)
    end

    poll_metrics()
    {:noreply, state}
  end

  defp poll_metrics(every \\ @poll_every) do
    Process.send_after(self(), :poll_metrics, every)
  end
end
