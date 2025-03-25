defmodule Logflare.GenSingleton do
  @moduledoc """
  A generic singleton GenServer that will be unique cluster-wide.
  Checks if there the server is started, if not, will be started under the supervision tree as a transient GenServer.
  """
  use GenServer

  @default_check 5_000

  require Logger
  @type state :: any()

  @spec start_link(args :: any()) :: {:ok, pid} | {:error, any}
  def start_link(args) do
    GenServer.start_link(__MODULE__, args, [])
  end

  def count_children(pid) do
    GenServer.call(pid, :count_children)
  end

  @impl true
  def init(args) do
    interval = args[:interval] || @default_check
    {:ok, pid} = Supervisor.start_link([], strategy: :one_for_one)

    Process.send_after(self(), :check, interval)

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

    {:ok, %{pid: pid, name: name, interval: interval, child_spec: args[:child_spec]},
     {:continue, :maybe_start_child}}
  end

  @impl true
  def handle_continue(:maybe_start_child, state) do
    try_start_child(state)
    {:noreply, state}
  end

  @impl true
  def handle_info(:check, state) do
    try_start_child(state)
    # Reschedule the next check
    Process.send_after(self(), :check, state.interval)
    {:noreply, state}
  end

  defp try_start_child(state) do
    with nil <- GenServer.whereis(state.name),
         {:ok, _pid} <- Supervisor.start_child(state.pid, state.child_spec) do
      :ok
    else
      {:error, {:already_started, pid}} ->
        Logger.debug(
          "GenSingleton | process #{inspect(pid)} is already started on server: #{inspect(node(pid))}"
        )

        pid

      pid when is_pid(pid) ->
        Logger.debug(
          "GenSingleton | process #{inspect(pid)} is already started on server: #{inspect(node(pid))}"
        )

        pid

      other ->
        other
    end
  end

  @impl true
  def handle_call(:count_children, _from, state) do
    {:reply, Supervisor.count_children(state.pid).active, state}
  end

  @impl true
  def terminate(_reason, _state) do
    :ok
  end
end
