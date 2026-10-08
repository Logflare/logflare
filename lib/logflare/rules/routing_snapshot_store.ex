defmodule Logflare.Rules.RoutingSnapshotStore do
  @moduledoc """
  Disposable, generation-qualified ETS acceleration for routing snapshots.

  The production table has a stable name across restarts. Each source has at most
  one complete target tuple; older readers always retain their exact compressed
  fallback. Restore never writes Cachex headers or evicts another source. Newer
  resident generations win, and duplicate restores do not extend their TTL.

  Restore admission uses a fixed number of hash slots. Concurrent requests for
  the same source coalesce; collisions and capacity pressure simply leave readers
  on their batch-local fallback. Requests carry only the compressed backup, and
  decoding happens after admission in the store. Periodic pruning releases claims
  abandoned by a reader that dies before sending its request.

  Source count and conservative estimated weights bound resident acceleration,
  with a soft byte limit for one oversized cold publication. Trees and compressed
  backups in Cachex are independently subject to its entry limit and TTL, not this
  store's byte budget. Acquired reader snapshots and queued restores are also
  outside that budget. Expiry, eviction and publisher death never retire headers.
  """

  use GenServer

  @default_limit 100_000
  @default_max_bytes 512 * 1024 * 1024
  @default_ttl :timer.hours(1)
  @default_interval :timer.minutes(5)
  @default_restore_slots 256
  @restore_requests_key :restore_requests
  @telemetry_event [:logflare, :rules, :routing_snapshot_store]

  @type key() :: {integer(), pos_integer()}
  @type table() :: atom() | :ets.tid()

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    super(Keyword.put_new(opts, :name, __MODULE__))
  end

  @spec put(GenServer.server(), key(), tuple(), non_neg_integer()) :: table()
  def put(server, key, targets, estimated_bytes) do
    GenServer.call(server, {:put, key, targets, estimated_bytes})
  end

  @spec restore(GenServer.server(), table(), key(), binary(), non_neg_integer()) :: :ok
  def restore(server, table, {source_id, _generation} = key, encoded, estimated_bytes) do
    case :ets.lookup(table, @restore_requests_key) do
      [{@restore_requests_key, requests, slots}] ->
        slot = :erlang.phash2(source_id, slots)
        claim = {slot, key, make_ref()}

        if :ets.insert_new(requests, claim) do
          GenServer.cast(server, {:restore, requests, claim, encoded, estimated_bytes})
        end

      [] ->
        :ok
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  @spec delete(GenServer.server(), key()) :: :ok
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
    table_name = Keyword.get(opts, :table, __MODULE__)
    table_opts = [:set, :protected, read_concurrency: true]
    table_opts = if table_name, do: [:named_table | table_opts], else: table_opts
    table = :ets.new(table_name || __MODULE__, table_opts)
    requests = :ets.new(__MODULE__, [:set, :public, write_concurrency: true])

    :ets.insert(table, {
      @restore_requests_key,
      requests,
      Keyword.get(opts, :restore_slots, @default_restore_slots)
    })

    state = %{
      table: table,
      requests: requests,
      sources: :ets.new(__MODULE__, [:set, :private]),
      expiry: :ets.new(__MODULE__, [:ordered_set, :private]),
      limit: Keyword.get(opts, :limit, @default_limit),
      max_bytes: Keyword.get(opts, :max_bytes, @default_max_bytes),
      estimated_bytes: 0,
      ttl: Keyword.get(opts, :ttl, @default_ttl),
      interval: Keyword.get(opts, :interval, @default_interval)
    }

    schedule_prune(state)
    emit(state, :start)
    {:ok, state}
  end

  @impl true
  def handle_call({:put, {source_id, generation} = key, targets, estimated_bytes}, _from, state) do
    state =
      if newer_resident?(state, source_id, generation) do
        state
      else
        state
        |> remove_source(source_id)
        |> insert_source(key, targets, estimated_bytes)
        |> trim(System.monotonic_time(:millisecond))
      end

    emit(state, :put)
    {:reply, state.table, state}
  end

  def handle_call(:prune, _from, state) do
    state = prune_state(state)
    {:reply, :ok, state}
  end

  @impl true
  def handle_cast(
        {:restore, requests, {slot, {source_id, generation} = key, _token} = claim, encoded,
         estimated_bytes},
        state
      ) do
    state =
      if requests == state.requests and :ets.lookup(requests, slot) == [claim] do
        state = trim(state, System.monotonic_time(:millisecond))

        if newer_resident?(state, source_id, generation) or
             not restore_fits?(state, source_id, estimated_bytes) do
          state
        else
          targets = decode_targets(encoded)

          state =
            state |> remove_source(source_id) |> insert_source(key, targets, estimated_bytes)

          emit(state, :restore)
          state
        end
      else
        state
      end

    if requests == state.requests, do: :ets.delete_object(requests, claim)
    {:noreply, state}
  end

  def handle_cast({:delete, {source_id, _generation} = key}, state) do
    state =
      case :ets.lookup(state.sources, source_id) do
        [{^source_id, ^key, _expires_at, _estimated_bytes}] ->
          state = remove_source(state, source_id)
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
    state = prune_state(state)
    schedule_prune(state)
    {:noreply, state}
  end

  @spec newer_resident?(map(), integer(), pos_integer()) :: boolean()
  defp newer_resident?(state, source_id, generation) do
    case :ets.lookup(state.sources, source_id) do
      [{^source_id, {^source_id, resident}, _expires_at, _bytes}] -> resident >= generation
      [] -> false
    end
  end

  @spec restore_fits?(map(), integer(), non_neg_integer()) :: boolean()
  defp restore_fits?(state, source_id, estimated_bytes) do
    {extra_source, previous_bytes} =
      case :ets.lookup(state.sources, source_id) do
        [{^source_id, _key, _expires_at, bytes}] -> {0, bytes}
        [] -> {1, 0}
      end

    :ets.info(state.sources, :size) + extra_source <= state.limit and
      (state.max_bytes == :infinity or
         state.estimated_bytes - previous_bytes + estimated_bytes <= state.max_bytes)
  end

  @spec decode_targets(binary()) :: tuple()
  defp decode_targets(encoded) do
    case :erlang.binary_to_term(encoded) do
      targets when is_map(targets) -> targets |> Enum.sort_by(&elem(&1, 0)) |> List.to_tuple()
      targets when is_tuple(targets) -> targets
    end
  end

  @spec insert_source(map(), key(), tuple(), non_neg_integer()) :: map()
  defp insert_source(state, {source_id, _generation} = key, targets, estimated_bytes) do
    expires_at = System.monotonic_time(:millisecond) + state.ttl
    :ets.insert(state.table, Tuple.insert_at(targets, 0, key))
    :ets.insert(state.sources, {source_id, key, expires_at, estimated_bytes})
    :ets.insert(state.expiry, {{expires_at, key}})
    Map.update!(state, :estimated_bytes, &(&1 + estimated_bytes))
  end

  @spec remove_source(map(), integer()) :: map()
  defp remove_source(state, source_id) do
    case :ets.take(state.sources, source_id) do
      [{^source_id, key, expires_at, estimated_bytes}] ->
        :ets.delete(state.table, key)
        :ets.delete(state.expiry, {expires_at, key})
        Map.update!(state, :estimated_bytes, &(&1 - estimated_bytes))

      [] ->
        state
    end
  end

  @spec prune_state(map()) :: map()
  defp prune_state(state) do
    :ets.delete_all_objects(state.requests)
    trim(state, System.monotonic_time(:millisecond))
  end

  @spec trim(map(), integer()) :: map()
  defp trim(state, now) do
    case :ets.first(state.expiry) do
      {expires_at, {source_id, _generation}} ->
        size = :ets.info(state.sources, :size)

        reason =
          cond do
            expires_at <= now -> :expire
            size > state.limit -> :evict
            over_byte_limit?(state, size) -> :evict
            true -> nil
          end

        if reason do
          state = remove_source(state, source_id)
          emit(state, reason)
          trim(state, now)
        else
          state
        end

      :"$end_of_table" ->
        state
    end
  end

  @spec over_byte_limit?(map(), non_neg_integer()) :: boolean()
  defp over_byte_limit?(%{max_bytes: :infinity}, _size), do: false

  defp over_byte_limit?(state, size) do
    state.estimated_bytes > state.max_bytes and size > 1
  end

  @spec emit(map(), atom()) :: :ok
  defp emit(state, action) do
    :telemetry.execute(
      @telemetry_event,
      %{
        sources: :ets.info(state.sources, :size),
        estimated_bytes: state.estimated_bytes
      },
      %{action: action}
    )
  end

  @spec schedule_prune(map()) :: reference()
  defp schedule_prune(state), do: Process.send_after(self(), :prune, state.interval)
end
