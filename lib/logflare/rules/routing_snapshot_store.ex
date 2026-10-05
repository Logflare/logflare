defmodule Logflare.Rules.RoutingSnapshotStore do
  @moduledoc """
  Byte-aware, disposable ETS acceleration for routing snapshots.

  There is at most one complete target tuple per source. Replacing a source never
  mixes generations: older readers use the immutable binary in their header.
  Store restart, age-based expiry and capacity eviction affect correctness only
  through a slower fallback, and the still-current header is rehydrated after
  that first fallback.

  Short-lived Cachex publishers are monitored until they finish committing the
  header. Retirement of a pending generation completes after its publisher exits,
  including abnormal exits; header deletion runs outside the server and compares
  generations so it cannot delete a replacement. Routing reads never call the
  server. Capacity is bounded by both source count and estimated bytes. Byte
  estimates include the tree, compact target tuple and compressed fallback;
  the bound is soft for one individually oversized snapshot so it remains usable.
  """

  use GenServer

  alias Logflare.Rules.Cache
  alias Logflare.Utils.Tasks

  @default_limit 100_000
  @default_max_bytes 512 * 1024 * 1024
  @default_ttl :timer.hours(1)
  @default_interval :timer.minutes(5)
  @telemetry_event [:logflare, :rules, :routing_snapshot_store]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    super(Keyword.put_new(opts, :name, __MODULE__))
  end

  @spec put(GenServer.server(), integer(), tuple(), non_neg_integer(), pid() | nil) ::
          {:ets.tid(), {integer(), reference()}}
  def put(server, source_id, targets, estimated_bytes, publisher \\ nil) do
    GenServer.call(server, {:put, source_id, targets, estimated_bytes, publisher})
  end

  @spec delete(GenServer.server(), {integer(), reference()}) :: :ok
  def delete(server, key), do: GenServer.cast(server, {:delete, key})

  @doc false
  @spec prune(GenServer.server()) :: :ok
  def prune(server), do: GenServer.call(server, :prune)

  @doc false
  @spec emit_metrics(GenServer.server()) :: :ok
  def emit_metrics(server \\ __MODULE__) do
    pid = GenServer.whereis(server)

    if is_pid(pid) and Process.alive?(pid) do
      GenServer.cast(pid, :emit_metrics)
    else
      :telemetry.execute(@telemetry_event, %{sources: 0, estimated_bytes: 0}, %{
        action: :unavailable
      })
    end
  end

  @impl true
  def init(opts) do
    state = %{
      table: :ets.new(__MODULE__, [:set, :protected, read_concurrency: true]),
      sources: :ets.new(__MODULE__, [:set, :private]),
      expiry: :ets.new(__MODULE__, [:ordered_set, :private]),
      limit: Keyword.get(opts, :limit, @default_limit),
      max_bytes: Keyword.get(opts, :max_bytes, @default_max_bytes),
      estimated_bytes: 0,
      publishers: %{},
      ttl: Keyword.get(opts, :ttl, @default_ttl),
      interval: Keyword.get(opts, :interval, @default_interval)
    }

    schedule_prune(state)
    emit(state, :start)
    {:ok, state}
  end

  @impl true
  def handle_call({:put, source_id, targets, estimated_bytes, publisher}, _from, state) do
    state = remove_source(state, source_id, :replace)
    key = {source_id, make_ref()}
    {state, monitor} = monitor_publisher(state, publisher, key)
    expires_at = System.monotonic_time(:millisecond) + state.ttl
    estimated_bytes = max(estimated_bytes, 0)

    :ets.insert(state.table, Tuple.insert_at(targets, 0, key))
    :ets.insert(state.sources, {source_id, key, expires_at, estimated_bytes, monitor})
    :ets.insert(state.expiry, {{expires_at, key}})

    state =
      state
      |> Map.update!(:estimated_bytes, &(&1 + estimated_bytes))
      |> trim(System.monotonic_time(:millisecond))

    emit(state, :put)
    {:reply, {state.table, key}, state}
  end

  def handle_call(:prune, _from, state) do
    state = trim(state, System.monotonic_time(:millisecond))
    {:reply, :ok, state}
  end

  @impl true
  def handle_cast({:delete, {source_id, _generation} = key}, state) do
    state =
      case :ets.lookup(state.sources, source_id) do
        [{^source_id, ^key, _expires_at, _estimated_bytes, _monitor}] ->
          state = remove_source(state, source_id, :delete)
          emit(state, :delete)
          state

        _ ->
          state
      end

    {:noreply, state}
  end

  def handle_cast(:emit_metrics, state) do
    emit(state, :poll)
    {:noreply, state}
  end

  @impl true
  def handle_info(:prune, state) do
    state = trim(state, System.monotonic_time(:millisecond))
    schedule_prune(state)
    {:noreply, state}
  end

  def handle_info({:DOWN, monitor, :process, _publisher, reason}, state) do
    case Map.pop(state.publishers, monitor) do
      {nil, _publishers} ->
        {:noreply, state}

      {{source_id, _generation} = key, publishers} ->
        state = %{state | publishers: publishers}

        case :ets.lookup(state.sources, source_id) do
          [{^source_id, ^key, _expires_at, _bytes, ^monitor}] when reason == :normal ->
            :ets.update_element(state.sources, source_id, {5, nil})
            {:noreply, state}

          [{^source_id, ^key, _expires_at, _bytes, ^monitor}] ->
            state = remove_source(state, source_id, :abort)
            retire_header(key)
            emit(state, :abort)
            {:noreply, state}

          _ ->
            retire_header(key)
            {:noreply, state}
        end
    end
  end

  @spec monitor_publisher(map(), pid() | nil, {integer(), reference()}) ::
          {map(), reference() | nil}
  defp monitor_publisher(state, nil, _key), do: {state, nil}

  defp monitor_publisher(state, publisher, key) do
    monitor = Process.monitor(publisher)
    {%{state | publishers: Map.put(state.publishers, monitor, key)}, monitor}
  end

  defp remove_source(state, source_id, reason) do
    case :ets.take(state.sources, source_id) do
      [{^source_id, key, expires_at, estimated_bytes, monitor}] ->
        :ets.delete(state.table, key)
        :ets.delete(state.expiry, {expires_at, key})

        state = Map.update!(state, :estimated_bytes, &max(&1 - estimated_bytes, 0))
        if monitor == nil, do: maybe_delete_header(reason, key)
        state

      [] ->
        state
    end
  end

  defp trim(state, now) do
    case :ets.first(state.expiry) do
      {expires_at, {source_id, _generation}} ->
        size = :ets.info(state.table, :size)

        reason =
          cond do
            expires_at <= now -> :expire
            size > state.limit -> :evict
            over_byte_limit?(state, size) -> :evict
            true -> nil
          end

        if reason do
          state = remove_source(state, source_id, reason)
          emit(state, reason)
          trim(state, now)
        else
          state
        end

      :"$end_of_table" ->
        state
    end
  end

  defp over_byte_limit?(%{max_bytes: :infinity}, _size), do: false

  defp over_byte_limit?(state, size) do
    state.estimated_bytes > state.max_bytes and size > 1
  end

  defp maybe_delete_header(reason, key) when reason in [:expire, :evict],
    do: retire_header(key)

  defp maybe_delete_header(_reason, _key), do: :ok

  @spec retire_header({integer(), reference()}) :: :ok
  defp retire_header(key) do
    Tasks.start_child(fn -> Cache.delete_routing_snapshot(key) end)
    :ok
  end

  defp emit(state, action) do
    :telemetry.execute(
      @telemetry_event,
      %{
        sources: :ets.info(state.table, :size),
        estimated_bytes: state.estimated_bytes
      },
      %{action: action}
    )
  end

  defp schedule_prune(state), do: Process.send_after(self(), :prune, state.interval)
end
