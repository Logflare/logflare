defmodule Logflare.Backends.UserMonitoring.SystemSourceStarter do
  @moduledoc """
  Holds the log events for a system logs source whose `SourceSup` is down. It starts that
  `SourceSup` outside the process that logs, then sends the held events to the source.

  `Logflare.Backends.UserMonitoring.log_interceptor/2` runs in the logging process. That process
  can be inside a `SourceSup` start. A synchronous start from there can wait on the same
  `SourcesSup` partition. That wait deadlocks the partition. Thus the interceptor casts the
  events to this process.

  This process does these steps for each source:

  1. It adds each event to a buffer for the source.
  2. It runs one start at a time in an unlinked task. Thus it never waits on a partition, and a
     failed start can not crash it.
  3. When the start succeeds, it sends the buffer to the source through `Processor.ingest/3` in
     an unlinked task.
  4. When the start fails, it keeps the buffer and tries the start again after the
     `:system_source_starter_retry_interval` application environment value. The default is
     5 seconds.

  This process drops events in two cases only. Each drop emits
  `[:logflare, :user_monitoring, :system_source_starter, :dropped]`.

  - The source does not exist (`reason: :not_found`). Nothing can receive the events.
  - The buffer holds the `:system_source_starter_max_buffer` application environment value of
    events (`reason: :buffer_full`). The default is 10,000 events per source. The cap keeps a
    log flood during a blocked partition from using all memory.
  """

  use GenServer

  alias Logflare.Backends
  alias Logflare.Logs
  alias Logflare.Logs.Processor
  alias Logflare.Sources
  alias Logflare.Sources.Source

  @default_max_buffer 10_000
  @default_retry_interval :timer.seconds(5)

  @type buffer :: {non_neg_integer(), [map()]}
  @type state :: %{
          in_flight: %{reference() => pos_integer()},
          retrying: MapSet.t(pos_integer()),
          buffers: %{pos_integer() => buffer()}
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Holds formatted log events for the source id until its `SourceSup` is up.

  The function returns at once. It does not wait for the start or the ingest.
  """
  @spec buffer(pos_integer(), [map()]) :: :ok
  def buffer(source_id, events) when is_integer(source_id) and is_list(events) do
    GenServer.cast(__MODULE__, {:buffer, source_id, events})
  end

  @impl GenServer
  @spec init(keyword()) :: {:ok, state()}
  def init(_opts), do: {:ok, %{in_flight: %{}, retrying: MapSet.new(), buffers: %{}}}

  @impl GenServer
  def handle_cast({:buffer, source_id, events}, state) do
    state
    |> add_to_buffer(source_id, events)
    |> maybe_start(source_id)
    |> then(&{:noreply, &1})
  end

  @impl GenServer
  def handle_info({ref, result}, state) when is_map_key(state.in_flight, ref) do
    Process.demonitor(ref, [:flush])
    {source_id, in_flight} = Map.pop!(state.in_flight, ref)
    {:noreply, handle_result(%{state | in_flight: in_flight}, source_id, result)}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state)
      when is_map_key(state.in_flight, ref) do
    {source_id, in_flight} = Map.pop!(state.in_flight, ref)
    {:noreply, handle_result(%{state | in_flight: in_flight}, source_id, {:error, reason})}
  end

  def handle_info({:retry, source_id}, state) do
    state = %{state | retrying: MapSet.delete(state.retrying, source_id)}

    if is_map_key(state.buffers, source_id) do
      {:noreply, maybe_start(state, source_id)}
    else
      {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  @spec add_to_buffer(state(), pos_integer(), [map()]) :: state()
  defp add_to_buffer(state, source_id, events) do
    max = Application.get_env(:logflare, :system_source_starter_max_buffer, @default_max_buffer)
    {count, held} = Map.get(state.buffers, source_id, {0, []})
    {kept, dropped} = Enum.split(events, max(max - count, 0))

    if dropped != [] do
      emit_dropped(source_id, length(dropped), :buffer_full)
    end

    buffer = {count + length(kept), Enum.reverse(kept, held)}
    %{state | buffers: Map.put(state.buffers, source_id, buffer)}
  end

  @spec maybe_start(state(), pos_integer()) :: state()
  defp maybe_start(state, source_id) do
    if source_id in Map.values(state.in_flight) or MapSet.member?(state.retrying, source_id) do
      state
    else
      %Task{ref: ref} = run_task(source_id, fn -> start(source_id) end)
      %{state | in_flight: Map.put(state.in_flight, ref, source_id)}
    end
  end

  @spec handle_result(state(), pos_integer(), :ok | {:error, term()}) :: state()
  defp handle_result(state, source_id, :ok) do
    {{_count, held}, buffers} = Map.pop(state.buffers, source_id, {0, []})

    if held != [] do
      run_task(source_id, fn -> ingest(source_id, Enum.reverse(held)) end)
    end

    %{state | buffers: buffers}
  end

  defp handle_result(state, source_id, {:error, :not_found}) do
    {{count, _held}, buffers} = Map.pop(state.buffers, source_id, {0, []})

    if count > 0 do
      emit_dropped(source_id, count, :not_found)
    end

    %{state | buffers: buffers}
  end

  defp handle_result(state, source_id, {:error, _reason}) do
    interval =
      Application.get_env(
        :logflare,
        :system_source_starter_retry_interval,
        @default_retry_interval
      )

    Process.send_after(self(), {:retry, source_id}, interval)
    %{state | retrying: MapSet.put(state.retrying, source_id)}
  end

  @spec run_task(pos_integer(), (-> term())) :: Task.t()
  defp run_task(source_id, fun) do
    Task.Supervisor.async_nolink(
      {:via, PartitionSupervisor, {Logflare.TaskSupervisors, source_id}},
      fun
    )
  end

  @spec start(pos_integer()) :: :ok | {:error, term()}
  defp start(source_id) do
    case Sources.Cache.get_by_id(source_id) do
      %Source{} = source -> Backends.ensure_source_sup_started(source)
      nil -> {:error, :not_found}
    end
  end

  @spec ingest(pos_integer(), [map()]) :: term()
  defp ingest(source_id, events) do
    case Sources.Cache.get_by_and_preload_rules(id: source_id) do
      %Source{} = source ->
        source
        |> Sources.refresh_source_metrics_for_ingest()
        |> then(&Processor.ingest(events, Logs.Raw, &1))

      nil ->
        emit_dropped(source_id, length(events), :not_found)
    end
  end

  @spec emit_dropped(pos_integer(), pos_integer(), :buffer_full | :not_found) :: :ok
  defp emit_dropped(source_id, count, reason) do
    :telemetry.execute(
      [:logflare, :user_monitoring, :system_source_starter, :dropped],
      %{count: count},
      %{source_id: source_id, reason: reason}
    )
  end
end
