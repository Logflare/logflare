defmodule Logflare.Backends.UserMonitoring.SystemSourceStarter do
  @moduledoc """
  Starts the `SourceSup` of a user's system logs source outside the process that logs.

  `Logflare.Backends.UserMonitoring.log_interceptor/2` runs in the logging process, which can be a
  process inside a `SourceSup` start. A synchronous start from there can wait on the same
  `SourcesSup` partition and deadlock it. The interceptor casts the source id here instead.

  Each start runs in an unlinked task, so this process never waits on a partition and a failed
  start can not crash it. Requests for an id whose start is still in flight, or whose `SourceSup`
  is already up, are skipped. A log flood therefore causes at most one start per source at a time.
  """

  use GenServer

  alias Logflare.Backends
  alias Logflare.Sources
  alias Logflare.Sources.Source

  @type state :: %{in_flight: %{reference() => pos_integer()}}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Requests a start of the `SourceSup` for the source id. Returns at once.
  """
  @spec request_start(pos_integer()) :: :ok
  def request_start(source_id) when is_integer(source_id) do
    GenServer.cast(__MODULE__, {:start, source_id})
  end

  @impl GenServer
  @spec init(keyword()) :: {:ok, state()}
  def init(_opts), do: {:ok, %{in_flight: %{}}}

  @impl GenServer
  def handle_cast({:start, source_id}, state) do
    if source_id in Map.values(state.in_flight) or Backends.source_sup_started?(source_id) do
      {:noreply, state}
    else
      %Task{ref: ref} =
        Task.Supervisor.async_nolink(
          {:via, PartitionSupervisor, {Logflare.TaskSupervisors, source_id}},
          fn -> start(source_id) end
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

  @spec start(pos_integer()) :: :ok | {:error, term()}
  defp start(source_id) do
    case Sources.Cache.get_by_id(source_id) do
      %Source{} = source -> Backends.ensure_source_sup_started(source)
      nil -> {:error, :not_found}
    end
  end
end
