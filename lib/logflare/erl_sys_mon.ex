defmodule Logflare.ErlSysMon do
  @moduledoc """
  Logs Erlang System Monitor events.
  """

  use GenServer

  require Logger

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def init(_args) do
    :erlang.system_monitor(self(), [
      :busy_dist_port,
      :busy_port,
      {:long_gc, 250},
      {:long_schedule, 100}
    ])

    {:ok, []}
  end

  def handle_info({:monitor, pid, _type, _meta} = msg, state) when is_pid(pid) do
    pid_info =
      pid
      |> Process.info(:dictionary)
      |> case do
        {:dictionary, dict} when is_list(dict) ->
          Keyword.take(dict, [:"$ancestors", :"$initial_call"])

        other ->
          other
      end

    Logger.warning(
      "#{__MODULE__} message: " <> inspect(msg) <> "|\n process info: #{inspect(pid_info)}"
    )

    {:noreply, state}
  end

  # fallback for ports etc
  def handle_info(msg, state) do
    Logger.warning("#{__MODULE__} message: #{inspect(msg)}")
  end
end
