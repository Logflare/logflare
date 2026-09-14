defmodule Logflare.Rules.RoutingSnapshotStore do
  @moduledoc """
  Byte-aware, disposable ETS acceleration for routing snapshots.

  There is at most one complete target tuple per source. Replacing a source never
  mixes generations: older readers use the immutable binary in their header.
  Store restart, age-based expiry and capacity eviction affect correctness only
  through a slower fallback, and the still-current header is rehydrated after
  that first fallback.

  Publication and retirement are serialized, but routing reads never call the
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

  @spec put(GenServer.server(), integer(), tuple(), non_neg_integer()) ::
          {:ets.tid(), {integer(), reference()}}
  def put(server, source_id, targets, estimated_bytes) do
    GenServer.call(server, {:put, source_id, targets, estimated_bytes})
  end

  @spec delete(GenServer.server(), {integer(), reference()}) :: :ok
  def delete(server, key), do: GenServer.cast(server, {:delete, key})

  @doc false
  @spec prune(GenServer.server()) :: :ok
  def prune(server), do: GenServer.call(server, :prune)

  @impl true
  def init(opts) do
    state = %{
      table: :ets.new(__MODULE__, [:set, :protected, read_concurrency: true]),
      sources: :ets.new(__MODULE__, [:set, :private]),
      expiry: :ets.new(__MODULE__, [:ordered_set, :private]),
      limit: Keyword.get(opts, :limit, @default_limit),
      max_bytes: Keyword.get(opts, :max_bytes, @default_max_bytes),
      estimated_bytes: 0,
      ttl: Keyword.get(opts, :ttl, @default_ttl),
      interval: Keyword.get(opts, :interval, @default_interval)
    }

    schedule_prune(state)
    {:ok, state}
  end

  @impl true
  def handle_call({:put, source_id, targets, estimated_bytes}, _from, state) do
    state = remove_source(state, source_id, :replace)
    key = {source_id, make_ref()}
    expires_at = System.monotonic_time(:millisecond) + state.ttl
    estimated_bytes = max(estimated_bytes, 0)

    :ets.insert(state.table, Tuple.insert_at(targets, 0, key))
    :ets.insert(state.sources, {source_id, key, expires_at, estimated_bytes})
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
        [{^source_id, ^key, _expires_at, _estimated_bytes}] ->
          state = remove_source(state, source_id, :delete)
          emit(state, :delete)
          state

        _ ->
          state
      end

    {:noreply, state}
  end

  @impl true
  def handle_info(:prune, state) do
    state = trim(state, System.monotonic_time(:millisecond))
    schedule_prune(state)
    {:noreply, state}
  end

  defp remove_source(state, source_id, reason) do
    case :ets.take(state.sources, source_id) do
      [{^source_id, key, expires_at, estimated_bytes}] ->
        :ets.delete(state.table, key)
        :ets.delete(state.expiry, {expires_at, key})

        state = Map.update!(state, :estimated_bytes, &max(&1 - estimated_bytes, 0))
        maybe_delete_header(reason, key)
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

  defp maybe_delete_header(reason, key) when reason in [:expire, :evict] do
    Tasks.start_child(fn -> Cache.delete_routing_snapshot(key) end)
    :ok
  end

  defp maybe_delete_header(_reason, _key), do: :ok

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
