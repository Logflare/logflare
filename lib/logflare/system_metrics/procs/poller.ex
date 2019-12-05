defmodule Logflare.SystemMetrics.Procs.Poller do
  @moduledoc """
  Polls process for process stats.

  Process calculations inspired by: https://github.com/sasa1977/demo_system/blob/master/example_system/lib/runtime.ex
  """

  use GenServer

  alias Logflare.SystemMetrics.Procs

  require Logger

  @poll_every 30_000

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  def init(_state) do
    poll_metrics()
    {:ok, %{last_processes: Procs.get_processes()}}
  end

  def handle_info(:poll_metrics, state) do
    current_processes = Procs.get_processes()
    processes = final_processes(state.last_processes, current_processes)

    if Application.get_env(:logflare, :env) == :prod do
      Logger.info("Process metrics!", processes: processes)
    end

    poll_metrics()
    {:noreply, %{last_processes: current_processes}}
  end

  defp poll_metrics() do
    Process.send_after(self(), :poll_metrics, @poll_every)
  end

  defp final_processes(
         %{
           process_list: last_processes_list,
           total_reductions: last_total_reductions
         },
         current_processes
       ) do
    current_processes_list = current_processes.process_list
    current_total_reductions = current_processes.total_reductions

    processes =
      Enum.map(
        current_processes_list,
        fn {name, reds} ->
          prev_reds = Map.get(last_processes_list, name, 0)
          reds_diff = reds - prev_reds
          total_reds_diff = current_total_reductions - last_total_reductions
          reds_percentage_int = Kernel.floor(reds_diff / total_reds_diff * 100)

          %{
            name: name,
            reds: reds_diff,
            total_reds: total_reds_diff,
            reds_percentage_int: reds_percentage_int
          }
        end
      )

    processes
    |> Enum.sort_by(& &1.reds, &>=/2)
    |> Stream.take(10)
    |> Enum.map(
      &%{
        name: &1.name,
        reds: &1.reds,
        total_reds: &1.total_reds,
        reds_percentage_int: &1.reds_percentage_int
      }
    )
  end
end
