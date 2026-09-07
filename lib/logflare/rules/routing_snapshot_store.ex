defmodule Logflare.Rules.RoutingSnapshotStore do
  @moduledoc """
  Bounded, disposable ETS acceleration for routing snapshots.

  There is at most one complete rule tuple per source, published with one ETS
  insert. Reads bind both source and generation, and copy only selected elements.
  Replacing a source never mixes generations: older readers use the immutable
  binary in their header. Store restart, age-based expiry and capacity eviction
  likewise affect performance only, not snapshot correctness.

  Publication/retirement is serialized, but routing reads never call the server.
  The source and expiry indexes each have exactly one entry per source; neither
  repeated rebuilds nor suspended readers accumulate retired generations here.
  """

  use GenServer

  @default_limit 100_000
  @default_ttl :timer.hours(1)
  @default_interval :timer.minutes(5)

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    super(Keyword.put_new(opts, :name, __MODULE__))
  end

  @spec put(GenServer.server(), integer(), [{integer(), term()}]) ::
          {:ets.tid(), {integer(), reference()}}
  def put(server, source_id, entries) do
    GenServer.call(server, {:put, source_id, entries})
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
      ttl: Keyword.get(opts, :ttl, @default_ttl),
      interval: Keyword.get(opts, :interval, @default_interval)
    }

    schedule_prune(state)
    {:ok, state}
  end

  @impl true
  def handle_call({:put, source_id, entries}, _from, state) do
    remove_source(state, source_id)
    key = {source_id, make_ref()}
    expires_at = System.monotonic_time(:millisecond) + state.ttl
    :ets.insert(state.table, List.to_tuple([key | entries]))
    :ets.insert(state.sources, {source_id, key, expires_at})
    :ets.insert(state.expiry, {{expires_at, key}})
    trim(state, System.monotonic_time(:millisecond))
    {:reply, {state.table, key}, state}
  end

  def handle_call(:prune, _from, state) do
    trim(state, System.monotonic_time(:millisecond))
    {:reply, :ok, state}
  end

  @impl true
  def handle_cast({:delete, {source_id, _generation} = key}, state) do
    case :ets.lookup(state.sources, source_id) do
      [{^source_id, ^key, _expires_at}] -> remove_source(state, source_id)
      _ -> :ok
    end

    {:noreply, state}
  end

  @impl true
  def handle_info(:prune, state) do
    trim(state, System.monotonic_time(:millisecond))
    schedule_prune(state)
    {:noreply, state}
  end

  defp remove_source(state, source_id) do
    case :ets.take(state.sources, source_id) do
      [{^source_id, key, expires_at}] ->
        :ets.delete(state.table, key)
        :ets.delete(state.expiry, {expires_at, key})

      [] ->
        :ok
    end
  end

  defp trim(state, now) do
    case :ets.first(state.expiry) do
      {expires_at, {source_id, _generation}} ->
        if expires_at <= now or :ets.info(state.table, :size) > state.limit do
          remove_source(state, source_id)
          trim(state, now)
        end

      :"$end_of_table" ->
        :ok
    end
  end

  defp schedule_prune(state), do: Process.send_after(self(), :prune, state.interval)
end
