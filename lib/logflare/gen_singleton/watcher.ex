defmodule Logflare.GenSingleton.Watcher do
  @moduledoc """
  A generic singleton GenServer that will be unique cluster-wide.
  Checks if there the server is started, if not, will be started under the supervision tree as a transient GenServer.
  Monitors the global process and restarts it if it terminates.
  """

  use GenServer

  require Logger
  @type option :: {:child_spec, Supervisor.child_spec()}
  @type options :: [option()]

  @spec start_link(options()) :: {:ok, pid} | {:error, any}

  def start_link(args) do
    GenServer.start_link(__MODULE__, {self(), args}, [])
  end

  @doc """
  Get the pid of the global singleton process. it will return the same pid for all nodes in cluster.
  """
  @spec get_pid(pid()) :: pid() | nil
  def get_pid(pid) do
    GenServer.call(pid, :get_pid)
  end

  @impl true
  def init({sup_pid, args}) do
    {:ok,
     %{
       sup_pid: sup_pid,
       child_spec: args[:child_spec],
       monitor_ref: nil,
       monitor_pid: nil
     }, {:continue, :maybe_start_child}}
  end

  @impl true
  def handle_continue(:maybe_start_child, state) do
    state = try_start_child(state)
    {:noreply, state}
  end

  @impl true
  def handle_info(:maybe_start_child, state) do
    state = try_start_child(state)
    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) when ref == state.monitor_ref do
    Logger.debug(
      "GenSingleton | monitor_pid DOWN received for #{inspect(state.monitor_pid)}, scheduling check..."
    )

    # delay check, but not all at the same time
    delay =
      if Application.get_env(:logflare, :env) == :test,
        do: 100,
        else: 3_000 + Enum.random(0..2_500)

    Process.send_after(self(), :maybe_start_child, delay)
    {:noreply, state}
  end

  defp try_start_child(%{child_spec: child_spec, sup_pid: sup_pid} = state) do
    spec = Supervisor.child_spec(child_spec, [])

    pid =
      case Supervisor.start_child(sup_pid, spec)
           |> then(fn
             {:error, :already_present} ->
               Supervisor.restart_child(sup_pid, spec.id)

             other ->
               other
           end) do
        {:error, {:syn_resolve_kill, _spec}} ->
          Logger.debug("GenSingleton | Process conflict detected, child did not start")
          nil

        {:error, {:already_started, pid}} ->
          Logger.debug(
            "GenSingleton | process #{inspect(pid)} is already started on server: #{inspect(node(pid))}"
          )

          pid

        {:ok, pid} when is_pid(pid) ->
          Logger.debug(
            "GenSingleton | process #{inspect(pid)} was started on server: #{inspect(node(pid))}"
          )

          pid

        other ->
          Logger.warning("GenSingleton unknown case | failed to start child: #{inspect(other)}")
          nil
      end

    if pid do
      monitor_ref = Process.monitor(pid)
      %{state | monitor_ref: monitor_ref, monitor_pid: pid}
    else
      state
    end
  end

  @impl true
  def handle_call(:get_pid, _from, state) do
    {:reply, state.monitor_pid, state}
  end

  @impl true
  def terminate(_reason, _state) do
    :ok
  end
end
