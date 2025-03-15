defmodule Logflare.Alerting.Supervisor do
  @moduledoc false

  alias Logflare.Alerting.AlertsScheduler
  require Logger
  use GenServer
  @interval 5000
  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  @impl GenServer
  def init(_args) do
    {:ok, pid} = Supervisor.start_link([], name: __MODULE__.Sup, strategy: :one_for_one)
    Process.send_after(self(), :maybe_start_scheduler, 100)
    {:ok, %{pid: pid}}
  end

  @impl GenServer
  def handle_info(:maybe_start_scheduler, state) do
    pid =
      :syn.lookup(:alerting, AlertsScheduler)
      |> case do
        {pid, _} -> pid
        _ -> nil
      end

    if pid == nil do
      case try_start() do
        {:ok, pid} ->
          Logger.info("Started alerts scheduler on #{inspect(Node.self())}, pid: #{inspect(pid)}")
          :syn.join(:alerting, :scheduler, pid)

        {:error, {:already_started, pid}} ->
          Logger.debug("Alerts scheduler already started on #{inspect(node(pid))}")
      end
    else
      Logger.debug("Alerts scheduler already started on #{inspect(node(pid))}")
    end

    Process.send_after(self(), :maybe_start_scheduler, @interval)

    {:noreply, state}
  end

  def try_start() do
    Supervisor.start_child(
      Logflare.Alerting.Supervisor.Sup,
      {AlertsScheduler, name: scheduler_name()}
    )
  end

  @doc """
  Returns the alerts scheduler :via name used for syn registry.
  """
  def scheduler_name do
    ts = DateTime.utc_now() |> DateTime.to_unix(:nanosecond)
    # add nanosecond resolution for timestamp comparison
    {:via, :syn, {:alerting, Logflare.Alerting.AlertsScheduler, %{timestamp: ts}}}
  end
end
