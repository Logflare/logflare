defmodule Logflare.Backends.UserMonitoring.SystemSourceStarter do
  @moduledoc """
  Starts the `SourceSup` of a user's system logs source outside the process that logs.

  `Logflare.Backends.UserMonitoring.log_interceptor/2` copies internal Logflare log lines to the
  `system.logs` source of a user as async system log events. It runs in the logging process. That
  process can be inside a `SourceSup` start. A synchronous start from there can wait on the same
  `SourcesSup` partition. That wait deadlocks the partition. Thus the interceptor casts the source
  id to this process. This process does not hold any events.

  This process runs each start in an unlinked task. Thus it never waits on a partition. A failed
  start can not crash it. The task passes only the source id to
  `Logflare.Backends.ensure_source_sup_started/1`. The source lookup then runs inside the start
  deadline, so a stalled lookup can not hold the task forever.

  This process skips a request in two cases: a start for that id is in flight, or the `SourceSup`
  is already up. Thus a log flood causes at most one start per source at a time. After a failed
  start, the next intercepted log line requests a new start.
  """

  use GenServer

  alias Logflare.Backends

  @type state :: %{in_flight: %{reference() => pos_integer()}}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Asks for a start of the `SourceSup` of the source id.

  The function returns at once. It does not wait for the start.
  """
  @spec request_start(pos_integer()) :: :ok
  def request_start(source_id) when is_integer(source_id) do
    GenServer.cast(__MODULE__, {:start, source_id})
  end

  @doc """
  Returns an empty state. Tests use it to reset this process.
  """
  @spec empty_state() :: state()
  def empty_state, do: %{in_flight: %{}}

  @impl GenServer
  @spec init(keyword()) :: {:ok, state()}
  def init(_opts), do: {:ok, empty_state()}

  @impl GenServer
  def handle_cast({:start, source_id}, state) do
    if source_id in Map.values(state.in_flight) or Backends.source_sup_started?(source_id) do
      {:noreply, state}
    else
      %Task{ref: ref} =
        Task.Supervisor.async_nolink(
          {:via, PartitionSupervisor, {Logflare.TaskSupervisors, source_id}},
          fn -> Backends.ensure_source_sup_started(source_id) end
        )

      {:noreply, put_in(state, [:in_flight, ref], source_id)}
    end
  end

  @impl GenServer
  def handle_info({ref, _result}, state) when is_map_key(state.in_flight, ref) do
    Process.demonitor(ref, [:flush])
    {:noreply, %{state | in_flight: Map.delete(state.in_flight, ref)}}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    {:noreply, %{state | in_flight: Map.delete(state.in_flight, ref)}}
  end

  def handle_info(_message, state), do: {:noreply, state}
end
