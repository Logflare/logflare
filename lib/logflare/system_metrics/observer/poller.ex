defmodule Logflare.SystemMetrics.Observer.Poller do
  use GenServer

  alias Logflare.SystemMetrics.Observer

  require Logger

  @poll_every 1_000

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  def init(_state) do
    poll_metrics()
    {:ok, %{last_processes: Observer.get_processes()}}
  end

  def handle_info(:poll_metrics, state) do
    poll_metrics()
    observer_metrics = Observer.get_metrics()
    observer_memory = Observer.get_memory()
    processes = final_processes(state.last_processes)

    LogflareLogger.info("Memory metrics!", observer_memory: observer_memory)
    LogflareLogger.info("Process metrics!", processes: processes)
    LogflareLogger.info("Observer metrics!", observer_metrics: observer_metrics)

    {:noreply, %{last_processes: Observer.get_processes()}}
  end

  defp poll_metrics() do
    Process.send_after(self(), :poll_metrics, @poll_every)
  end

  defp final_processes(last_processes) do
    processes =
      Enum.map(
        Observer.get_processes(),
        fn {name, reds} ->
          prev_reds = Map.get(last_processes, name, 0)
          %{name: name, reds: reds - prev_reds}
        end
      )

    processes
    |> Enum.sort_by(& &1.reds, &>=/2)
    |> Stream.take(10)
    |> Enum.map(&%{name: &1.name, reds: &1.reds})
  end
end
