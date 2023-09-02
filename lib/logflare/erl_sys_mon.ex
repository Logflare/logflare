defmodule Logflare.ErlSysMon do
  @moduledoc """
  Logs Erlang System Monitor events.
  """

  require Logger

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def init(_args) do
    :erlang.system_monitor(self(), [
      :busy_dist_port,
      :busy_port,
      {:long_gc, 100},
      {:long_schedule, 100}
    ])

    {:ok, []}
  end

  def handle_info(msg, state) do
    Logger.warning("#{__MODULE__} message: " <> inspect(msg))

    {:noreply, state}
  end
end
