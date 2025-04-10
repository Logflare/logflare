defmodule Logflare.GenSingleton do
  @moduledoc """
  A generic singleton GenServer that will be unique cluster-wide.
  Checks if there the server is started, if not, will be started under the supervision tree as a transient GenServer.
  """
  use GenServer

  require Logger
  @type state :: any()

  @spec start_link(args :: any()) :: {:ok, pid} | {:error, any}

  def start_link(args) do
    GenServer.start_link(__MODULE__, {self(), args}, [])
  end

  def get_pid(pid) do
    GenServer.call(pid, :get_pid)
  end

  @impl true
  def init({sup_pid, args}) do
    name =
      if args[:name] do
        args[:name]
      else
        case args[:child_spec] do
          {_mod, args} when is_list(args) -> Keyword.get(args, :name)
          %{start: {_mod, _func, args}} when is_list(args) -> Keyword.get(args, :name)
          mod when is_atom(mod) -> args[:name] || mod
        end
      end

    {:ok,
     %{
       sup_pid: sup_pid,
       name: name,
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
    Process.send_after(self(), :maybe_start_child, 4_000 + Enum.random(0..1_500))
    {:noreply, state}
  end

  defp try_start_child(state) do
    pid =
      case Supervisor.start_child(state.sup_pid, state.child_spec) do
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
