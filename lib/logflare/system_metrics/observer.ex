defmodule Logflare.SystemMetrics.Observer do
  use GenServer

  require Logger

  @send_every 1_000

  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_state) do
    state = get_observer_metrics()
    send_it()
    {:ok, state}
  end

  def handle_info(:send_it, _state) do
    state = get_observer_metrics()

    LogflareLogger.merge_context(observer_metrics: state)
    Logger.error("Observer metrics!")
    send_it()
    {:noreply, state}
  end

  defp send_it() do
    Process.send_after(self(), :send_it, @send_every)
  end

  defp get_observer_metrics() do
    :observer_backend.sys_info()
    |> Keyword.drop([:alloc_info])
    |> Enum.map(fn {x, y} ->
      if is_list(y) do
        {x, to_string(y)}
      else
        {x, y}
      end
    end)
  end
end
