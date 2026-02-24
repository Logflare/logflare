defmodule Logflare.ErlSysMon do
  @moduledoc """
  Logs Erlang System Monitor events.

  Also does system-related logging for debugging purposes.
  """

  use GenServer

  require Logger

  def start_link(args) do
    GenServer.start_link(__MODULE__, args,
      name: __MODULE__,
      hibernate_after: 5_000,
      spawn_opt: [fullsweep_after: 10_000]
    )
  end

  def init(_args) do
    :erlang.system_monitor(self(), [
      :busy_dist_port,
      :busy_port,
      {:long_gc, 1000},
      {:long_schedule, 500},
      {:long_message_queue, {0, 1_000}}
    ])

    # subscribe to
    :net_kernel.monitor_nodes(true, %{nodedown_reason: true})

    {:ok, []}
  end

  # allows setting of log level for runtime debugging
  def set_log_level(level) when is_atom(level) do
    GenServer.call(__MODULE__, {:put_level, level})
  end

  def handle_call({:put_level, level}, _from, state) do
    :ok = Logger.put_process_level(self(), level)

    {:reply, :ok, state}
  end

  def handle_info({node_status, node, info}, state) when node_status in [:nodeup, :nodedown] do
    Logger.debug(
      "ErlSysMon :net_kernel message - #{inspect(node_status)} for #{inspect(node)} | info: #{inspect(info)}"
    )

    {:noreply, state}
  end

  def handle_info({:monitor, pid, _type, _meta} = msg, state) when is_pid(pid) do
    message =
      "#{__MODULE__} message: #{inspect(msg)} |\n process info: #{inspect(get_process_info(pid))}"

    Logger.warning(message)

    {:noreply, state}
  end

  # fallback for ports etc
  def handle_info(msg, state) do
    Logger.warning("#{__MODULE__} message: #{inspect(msg)}")

    {:noreply, state}
  end

  defp get_process_info(pid) do
    pid
    |> Process.info(:dictionary)
    |> case do
      {:dictionary, dict} when is_list(dict) ->
        Keyword.take(dict, [:"$ancestors", :"$initial_call"])

      other ->
        other
    end
  end
end
