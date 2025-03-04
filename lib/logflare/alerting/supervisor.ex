defmodule Logflare.Alerting.Supervisor do
  @moduledoc false

  alias Logflare.Alerting.AlertsScheduler
  require Logger
  use GenServer
  @interval 5000
  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  @impl Supervisor
  def init(args) do
    {:ok, pid} = Supervisor.start_link([], name: __MODULE__.Sup, strategy: :one_for_one)
    Process.send_after(self(), :maybe_start_scheduler, 100)
    {:ok, %{pid: pid}}
  end

  def handle_info(:maybe_start_scheduler, state) do
    pid =
      :syn.lookup(:alerting, AlertsScheduler)
      |> case do
        {pid, _} -> pid
        _ -> nil
      end

    if pid == nil do
      case Supervisor.start_child(__MODULE__.Sup, {AlertsScheduler, name: scheduler_name()}) do
        {:ok, pid} ->
          Logger.info("Started alerts scheduler on #{inspect(Node.self())}")

        {:error, {:already_started, _pid}} ->
          Logger.debug("Alerts scheduler already started on #{inspect(node(pid))}")
      end
    else
      Logger.debug("Alerts scheduler already started on #{inspect(node(pid))}")
    end

    Process.send_after(self(), :maybe_start_scheduler, @interval)

    {:noreply, state}
  end

  @doc """
  Returns the alerts scheduler :via name used for syn registry.
  """
  def scheduler_name do
    {:via, :syn, {:alerting, Logflare.Alerting.AlertsScheduler}}
  end
end
