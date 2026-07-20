defmodule Logflare.Backends.IngestEventQueue do
  @moduledoc """
  GenServer will manage the ETS buffer mapping and own that table.

  :ets-backed buffer uses an :ets mapping pattern to fan out multiple :ets tables.

  Every queue uses the same storage model: a small per-pipeline "pointer" table (this
  module's main tables, one per `{sid,bid,pid}` / `{:consolidated,bid,pid}` /
  `{:spool_producer,nil,pid}`) holding lightweight pointer rows, plus a shared,
  generationally-rotated event store per `queues_key` holding the actual `LogEvent`
  bodies (see `current_generation_tid/1`, `new_generations/1`,
  `Logflare.Backends.IngestEventQueue.GenerationJanitor`). `add_to_table/3` always
  writes both; which read function a caller uses decides whether it gets back
  lightweight pointers (`pop_pending_pointers/2` — ID-passing pipelines: ClickHouse,
  BigQuery, spool) or fully-resolved `LogEvent` structs (`pop_pending/2` — every other
  adaptor).
  """
  use GenServer

  alias Logflare.Sources.Source
  alias Logflare.Backends.Backend
  alias Logflare.Backends.IngestEventQueue.LogEventPointer
  alias Logflare.LogEvent

  require Ex2ms

  @ets_table_mapper :ingest_event_queue_mapping
  @ets_table :source_ingest_events
  @ets_generations :ingest_event_queue_generations
  @ets_current_generation :ingest_event_queue_current_generation
  @ets_recent_events :ingest_event_queue_recent_events
  @max_queue_size 30_000
  @consolidated_max_queue_size 30_000
  @pointer_batch_key_match_spec (for freshness <- [:fresh, :stale],
                                     event_type <- [:log, :metric, :trace] do
                                   {{:_, :_, :_, :_, :_, event_type, :"$1", freshness},
                                    [{:is_integer, :"$1"}], [{{freshness, event_type, :"$1"}}]}
                                 end)

  @type source_backend :: {Source.t() | pos_integer(), Backend.t() | pos_integer() | nil}
  @type source_backend_pid ::
          {Source.t() | pos_integer(), Backend.t() | pos_integer() | nil, pid() | nil}
  @type table_key :: {pos_integer(), pos_integer() | nil, pid() | nil}
  @type table_obj :: {table_key(), :ets.tid()}
  @type queues_key :: {pos_integer(), pos_integer() | nil}
  @type consolidated_queues_key :: {:consolidated, pos_integer()}
  @type consolidated_table_key :: {:consolidated, pos_integer(), pid() | nil}
  @type spool_producer_queues_key :: {:spool_producer, nil}
  @type spool_producer_table_key :: {:spool_producer, nil, pid() | nil}
  @type pointer_batch_key ::
          {:fresh | :stale, LogEvent.TypeDetection.event_type(), integer()}

  defguardp is_pid_or_nil(value) when is_pid(value) or is_nil(value)

  def max_queue_size, do: @max_queue_size
  def max_consolidated_queue_size, do: @consolidated_max_queue_size

  @doc """
  Returns true if the key is a consolidated queue key.
  """
  @spec consolidated_key?(consolidated_queues_key() | consolidated_table_key() | term()) ::
          boolean()
  def consolidated_key?({:consolidated, _}), do: true
  def consolidated_key?({:consolidated, _, _}), do: true
  def consolidated_key?(_), do: false

  ## Server
  def start_link(_args) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__, hibernate_after: 1_000)
  end

  @impl GenServer
  def init(_args) do
    :ets.new(@ets_table_mapper, [
      :public,
      :named_table,
      :bag,
      {:write_concurrency, :auto},
      {:read_concurrency, false},
      {:decentralized_counters, false}
    ])

    :ets.new(@ets_generations, [
      :public,
      :named_table,
      :bag,
      {:write_concurrency, :auto},
      {:read_concurrency, true},
      {:decentralized_counters, false}
    ])

    :ets.new(@ets_current_generation, [
      :public,
      :named_table,
      :set,
      {:write_concurrency, :auto},
      {:read_concurrency, true},
      {:decentralized_counters, false}
    ])

    :ets.new(@ets_recent_events, [
      :public,
      :named_table,
      :ordered_set,
      {:write_concurrency, :auto},
      {:read_concurrency, true},
      {:decentralized_counters, false}
    ])

    {:ok, %{}}
  end

  # --- Generation store ---
  #
  # One shared event-data table per queues_key, rotated over time. "Current" is
  # published in @ets_current_generation (a plain ETS :set, read_concurrency: true)
  # so the hot insert path (add_to_table/3) never touches the GenServer to find it —
  # only creating a *new* generation does. Deliberately not :persistent_term: that
  # would make this read essentially free, but every :persistent_term.put/2 triggers a
  # full VM-wide GC sweep, and GenerationJanitor rewrites every live queues_key's
  # current generation on every tick, forever — at production scale (many thousands of
  # queues_keys) that's thousands of global sweeps back-to-back, every tick, which
  # would dwarf whatever the read-side savings bought. An ETS lookup here is still O(1)
  # and more than fast enough for this path.

  @doc """
  Returns the current generation's tid for `queues_key`, creating one if none exists yet.

  The lookup-miss check here is only a fast-path optimization to skip the GenServer
  call in the common case — it is NOT what prevents duplicate generations under
  concurrent first-time callers (many ingest requests can all observe a miss here
  before any of them has created one). That's instead enforced by
  `{:ensure_generation, _}` re-checking `@ets_current_generation` from inside the
  serialized `handle_call` before creating a table, so only the first caller actually
  creates one and the rest just read it back.
  """
  @spec current_generation_tid(
          queues_key()
          | consolidated_queues_key()
          | spool_producer_queues_key()
        ) :: :ets.tid()
  def current_generation_tid(queues_key) do
    case :ets.lookup(@ets_current_generation, queues_key) do
      [{^queues_key, tid}] -> tid
      [] -> GenServer.call(__MODULE__, {:ensure_generation, queues_key})
    end
  end

  @doc """
  Unconditionally creates a new generation table for each of `queues_keys` and makes
  each one current, even if one already exists — in a single GenServer call rather
  than one call per key, so rotating an entire fleet of queues doesn't mean that many
  sequential round-trips to the same serialized GenServer.

  Used by `GenerationJanitor` to rotate in fresh generations on its schedule — NOT by
  `current_generation_tid/1` (see `{:ensure_generation, _}` there instead), since
  calling this unconditionally from every first-time caller of a lazily-created
  generation would race: concurrent callers would each create their own table before
  any of them observes the others' write.
  """
  @spec new_generations([
          queues_key() | consolidated_queues_key() | spool_producer_queues_key()
        ]) :: :ok
  def new_generations(queues_keys) do
    GenServer.call(__MODULE__, {:new_generations, queues_keys})
  end

  @impl GenServer
  def handle_call({:new_generations, queues_keys}, _from, state) do
    for queues_key <- queues_keys, do: create_generation(queues_key)
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_call({:ensure_generation, queues_key}, _from, state) do
    tid =
      case :ets.lookup(@ets_current_generation, queues_key) do
        [{^queues_key, existing_tid}] -> existing_tid
        [] -> create_generation(queues_key)
      end

    {:reply, tid, state}
  end

  defp create_generation(queues_key) do
    tid =
      :ets.new(@ets_table, [
        :public,
        :set,
        {:decentralized_counters, false},
        {:write_concurrency, :auto},
        {:read_concurrency, true}
      ])

    :ets.insert(@ets_generations, {queues_key, tid, System.monotonic_time(:millisecond)})
    :ets.insert(@ets_current_generation, {queues_key, tid})
    tid
  end

  @doc """
  Lists every live generation (`{tid, created_at}`) for `queues_key`.
  """
  @spec list_generations(queues_key() | consolidated_queues_key() | spool_producer_queues_key()) ::
          [{:ets.tid(), integer()}]
  def list_generations(queues_key) do
    :ets.lookup(@ets_generations, queues_key)
    |> Enum.map(fn {_queues_key, tid, created_at} -> {tid, created_at} end)
  end

  @doc """
  Lists every distinct `queues_key` that currently has at least one live generation.

  Used by `GenerationJanitor` (a single global sweep, mirroring `MapperJanitor`'s
  precedent) to discover which queues need rotating/evicting without needing to know
  about specific backends or adaptors — the generations table itself is the source of
  truth for "what's currently using the generation store".
  """
  @spec list_generation_queues_keys() :: [
          queues_key() | consolidated_queues_key() | spool_producer_queues_key()
        ]
  def list_generation_queues_keys do
    :ets.foldl(
      fn {queues_key, _tid, _created_at}, acc -> MapSet.put(acc, queues_key) end,
      MapSet.new(),
      @ets_generations
    )
    |> MapSet.to_list()
  end

  @doc """
  Drops a generation: deletes its whole underlying table in one O(1) `:ets.delete/1` (not
  a per-row scan) and removes its bookkeeping row.

  Any pointer still referencing this generation's `tid` will simply miss on its next
  lookup (see `lookup_event/2`) — the same "not found" handling already used for any
  other lookup miss. This is the deliberate, bounded-loss cleanup mechanism for
  abandoned claims.
  """
  @spec drop_generation(
          queues_key() | consolidated_queues_key() | spool_producer_queues_key(),
          :ets.tid()
        ) :: :ok
  def drop_generation(queues_key, tid) do
    :ets.delete(tid)
    ms = [{{queues_key, tid, :_}, [], [true]}]
    :ets.select_delete(@ets_generations, ms)
    :ok
  rescue
    ArgumentError ->
      emit_stale_ets_table_telemetry()
      :ok
  end

  @doc """
  Drops every live generation for `queues_key` outright — not just aged ones — and
  clears its "current generation" entry, so a later revival creates a fresh table
  instead of returning a stale, already-dropped tid.

  Use this only once nothing references `queues_key` anymore (see
  `GenerationJanitor`, gated on `list_queues/1` returning `[]`) — this is what lets a
  queues_key whose owning source/backend supervisor has since died stop retaining
  ETS tables and bookkeeping entries forever (see
  github.com/Logflare/logflare/pull/3690#discussion_r3597179919). Any event still
  sitting in a pruned generation was already unclaimable: its pointer table died
  with the same owner, so there's nothing left anywhere that could still claim it.
  """
  @spec prune_generations(queues_key() | consolidated_queues_key() | spool_producer_queues_key()) ::
          :ok
  def prune_generations(queues_key) do
    for {tid, _created_at} <- list_generations(queues_key) do
      drop_generation(queues_key, tid)
    end

    :ets.delete(@ets_current_generation, queues_key)
    :ok
  end

  @doc """
  Looks up a single event directly in a generation table.

  `tid` here is a generation's tid (e.g. a claimed `LogEventPointer.tid`) — a direct
  `:ets.lookup/2`, no separate id-to-table resolution needed. Returns `nil` on a miss,
  including when the generation has already been dropped by rotation (see
  `drop_generation/2`) — callers should treat that the same as any other lookup miss,
  not as an error.
  """
  @spec lookup_event(:ets.tid(), term()) :: LogEvent.t() | nil
  def lookup_event(tid, id) do
    case :ets.lookup(tid, id) do
      [{^id, event}] -> event
      [] -> nil
    end
  rescue
    ArgumentError ->
      emit_stale_ets_table_telemetry()
      nil
  end

  # --- recent-events cache ---
  #
  # BigQuery's ack (the only real caller — see finalize_acked_events/2) always deletes
  # an acked pointer's generation-store row immediately, then separately records a full
  # copy of the event here (record_recent_event/2) when the source's ingest rate is low
  # enough to want "recent logs" visibility. Deliberately a copy, not a pointer:
  # generations are dropped by GenerationJanitor purely by age, with no regard for
  # whether some row inside is still referenced elsewhere, so a pointer-based cache
  # can't outlive its own generation — see the PR discussion this replaced
  # (github.com/Logflare/logflare/pull/3690#discussion_r3597505520). A copy has no such
  # dependency, so GenerationJanitor purges this cache on its own, independent (and much
  # longer) schedule — see recent_events_max_age_ms there, which needs to comfortably
  # cover Sources.source_idle?/1's 5-minute has_recent_logs_within?/2 dependency.
  #
  # fetch_events/2 merges this cache into its generation-store scan, so
  # list_recent_logs_local/2 sees a source's recently-acked events long after their
  # originating generation is gone, not just its still-pending/claimed ones.
  #
  # ClickHouse's ack deliberately never writes here: list_recent_logs_local/2
  # short-circuits to [] for any consolidated backend without reading this cache at
  # all (filtering by source in a consolidated queue would require scanning every
  # event), so recording would be a pure, unbounded, unread cost. BufferProducer's
  # do_pop_key (non-id-passing adaptors) doesn't need this either — it claims via
  # pop_pending/2, which atomically deletes the generation-store row as part of
  # claiming, and none of those adaptors need "recent logs" visibility.

  @doc """
  Records a copy of `event` into the recent-events cache for `queues_key`, independent
  of the generation store — it stays visible via `list_recent_events/2` (and therefore
  `fetch_events/2`) even after its originating generation has long since rotated out
  and been dropped.

  Use this after the caller has already deleted the event's generation-store row
  (BigQuery's ack, via `pop_pending_pointers/2` + `delete_id/2`) but still wants it to
  remain visible for a "recent logs" read.
  """
  @spec record_recent_event(
          queues_key() | consolidated_queues_key() | spool_producer_queues_key(),
          LogEvent.t()
        ) :: :ok
  def record_recent_event(queues_key, %LogEvent{} = event) do
    now = System.monotonic_time(:millisecond)
    key = {queues_key, now, System.unique_integer([:monotonic])}
    :ets.insert(@ets_recent_events, {key, event})
    :ok
  end

  @doc """
  Lists up to `n` recently-recorded event copies for `queues_key`, most-recently-recorded
  first.
  """
  @spec list_recent_events(
          queues_key() | consolidated_queues_key() | spool_producer_queues_key(),
          integer()
        ) :: [LogEvent.t()]
  def list_recent_events(queues_key, n) do
    ms = [{{{queues_key, :"$1", :"$2"}, :"$3"}, [], [:"$3"]}]

    case :ets.select_reverse(@ets_recent_events, ms, n) do
      {values, _cont} -> values
      :"$end_of_table" -> []
    end
  end

  @doc """
  Drops recent-events cache rows older than `max_age_ms`, across every `queues_key` in
  one pass. Called from `GenerationJanitor`'s periodic tick, on its own
  `recent_events_max_age_ms` schedule — independent of generation eviction's
  `max_age_ms`, since these rows no longer reference the generation store at all.
  """
  @spec sweep_recent_events(non_neg_integer()) :: :ok
  def sweep_recent_events(max_age_ms) do
    cutoff = System.monotonic_time(:millisecond) - max_age_ms

    ms = [{{{:_, :"$1", :_}, :_}, [{:<, :"$1", cutoff}], [true]}]
    :ets.select_delete(@ets_recent_events, ms)

    :ok
  end

  @doc """
  Trims the recent-events cache for a single `queues_key` down to at most `n` rows,
  keeping the most-recently-recorded ones. The count-bounded counterpart to
  `sweep_recent_events/1`'s age bound — this is what `QueueJanitor` calls per queue on
  its own schedule.
  """
  @spec truncate_recent(
          queues_key() | consolidated_queues_key() | spool_producer_queues_key(),
          non_neg_integer()
        ) :: :ok
  def truncate_recent(queues_key, n) do
    ms = [{{{queues_key, :"$1", :"$2"}, :_}, [], [{{:"$1", :"$2"}}]}]

    @ets_recent_events
    |> :ets.select_reverse(ms)
    |> Enum.drop(n)
    |> Enum.each(fn {ts, uniq} -> :ets.delete(@ets_recent_events, {queues_key, ts, uniq}) end)

    :ok
  end

  # --- pointer table mapper ---

  @doc """
  Retrieves a private tid of a given source-backend combination or consolidated queue.
  """
  @spec get_tid(table_key() | consolidated_table_key() | spool_producer_table_key()) ::
          :ets.tid() | nil
  def get_tid({:consolidated, bid, pid}) when is_integer(bid),
    do: do_get_tid({:consolidated, bid}, pid)

  def get_tid({:spool_producer, nil, pid}),
    do: do_get_tid({:spool_producer, nil}, pid)

  def get_tid({sid, bid, pid}) when is_integer(sid),
    do: do_get_tid({sid, bid}, pid)

  defp do_get_tid(key, pid) do
    :ets.match(@ets_table_mapper, {key, pid, :"$1"}, 1)
    |> case do
      {[[tid]], _cont} ->
        if :ets.info(tid, :name) != :undefined, do: tid

      _ ->
        nil
    end
  end

  @doc """
  Creates or updates a private :ets table. The :ets table mapper is stored in #{@ets_table_mapper} .
  """
  @spec upsert_tid(table_key() | consolidated_table_key() | spool_producer_table_key()) ::
          {:ok, :ets.tid()} | {:error, :already_exists, :ets.tid()}
  def upsert_tid({:consolidated, bid, pid} = key) when is_integer(bid) and is_pid_or_nil(pid) do
    do_upsert_tid(key, {:consolidated, bid}, pid)
  end

  def upsert_tid({:spool_producer, nil, pid} = key) when is_pid_or_nil(pid) do
    do_upsert_tid(key, {:spool_producer, nil}, pid)
  end

  def upsert_tid({sid, bid, pid} = sid_bid_pid)
      when is_integer(sid) and (is_integer(bid) or is_nil(bid)) and is_pid_or_nil(pid) do
    do_upsert_tid(sid_bid_pid, {sid, bid}, pid)
  end

  defp do_upsert_tid(key, mapper_key, pid) do
    case get_tid(key) do
      nil ->
        tid =
          :ets.new(@ets_table, [
            :public,
            :set,
            {:decentralized_counters, false},
            {:write_concurrency, :auto},
            {:read_concurrency, true}
          ])

        :ets.insert(@ets_table_mapper, {mapper_key, pid, tid})
        {:ok, tid}

      tid ->
        {:error, :already_exists, tid}
    end
  end

  @doc """
  Retrieves the table size of a given tid
  """
  @spec get_table_size(table_key() | consolidated_table_key()) :: integer() | nil
  def get_table_size(sid_bid_pid) do
    with tid when tid != nil <- get_tid(sid_bid_pid),
         num when is_integer(num) <- :ets.info(tid, :size) do
      num
    else
      _ -> nil
    end
  end

  @doc """
  Returns the sum of all pending events across all queues of a source-backend combination or consolidated queue.
  """
  @spec queues_pending_size(queues_key() | consolidated_queues_key()) :: integer() | nil
  def queues_pending_size(key) do
    list_pending_counts(key)
    |> Enum.reduce(0, fn {_table_key, count}, acc ->
      acc + count
    end)
  end

  @doc """
  Retrieves the :ets.info/1 of a table
  """
  @spec get_table_info(source_backend_pid()) :: list()
  def get_table_info({_source, _backend, _pid} = sbp) do
    get_tid(sbp)
    |> case do
      nil -> nil
      tid -> :ets.info(tid)
    end
  end

  @doc """
  Adds a record to a given source-backend's table queue.

  Always writes via the pointer + generation-store model (see moduledoc) — the full
  event body goes into the current generation for `queues_key`, and a lightweight
  pointer row goes into whichever pipeline's table the round-robin distribution below
  picks.
  """
  @spec add_to_table(
          source_backend_pid()
          | queues_key()
          | consolidated_queues_key()
          | consolidated_table_key()
          | spool_producer_queues_key()
          | spool_producer_table_key(),
          [LogEvent.t()],
          Keyword.t()
        ) :: :ok | {:error, :not_initialized}
  def add_to_table(sid_bid_or_sid_bid_pid, batch, opts \\ [])

  def add_to_table({:spool_producer, nil} = key, batch, opts) do
    chunk_size = Keyword.get(opts, :chunk_size, 100)
    startup_queue = {:spool_producer, nil, nil}

    reducer = fn
      {{:spool_producer, nil, nil}, _}, acc -> acc
      {obj, _count}, acc -> [obj | acc]
    end

    with all = [_ | _] <- list_counts_with_tids(key),
         available_queues = [_ | _] <- Enum.reduce(all, [], reducer) do
      Logflare.Utils.chunked_round_robin(
        batch,
        available_queues,
        chunk_size,
        &add_to_target_table/2
      )
    else
      _ ->
        add_to_table(startup_queue, batch)
    end

    :ok
  end

  def add_to_table({:consolidated, bid} = key, batch, opts) when is_integer(bid) do
    chunk_size = Keyword.get(opts, :chunk_size, 100)
    no_get_tid = Keyword.get(opts, :no_get_tid, true)
    check_queue_size = Keyword.get(opts, :check_queue_size, true)
    startup_queue = {:consolidated, bid, nil}

    reducer =
      if check_queue_size do
        fn
          {{:consolidated, _, nil}, _}, acc -> acc
          {_obj, count}, acc when count >= @consolidated_max_queue_size -> acc
          {obj, _count}, acc -> [obj | acc]
        end
      else
        fn
          {{:consolidated, _, nil}, _}, acc -> acc
          {obj, _count}, acc -> [obj | acc]
        end
      end

    if no_get_tid do
      with all = [_ | _] <- list_counts_with_tids(key),
           available_queues = [_ | _] <- Enum.reduce(all, [], reducer) do
        Logflare.Utils.chunked_round_robin(
          batch,
          available_queues,
          chunk_size,
          &add_to_target_table/2
        )
      else
        _ ->
          add_to_table(startup_queue, batch)
      end
    else
      proc_counts =
        list_counts(key)
        |> Enum.sort_by(fn {_key, count} -> count end, :asc)
        |> Enum.filter(fn
          {{:consolidated, _, nil}, _} -> false
          {{:consolidated, _, _}, _} -> true
        end)

      procs = Enum.map(proc_counts, fn {proc_key, _count} -> proc_key end)

      if procs == [] do
        add_to_table({:consolidated, bid, nil}, batch)
      else
        Logflare.Utils.chunked_round_robin(
          batch,
          procs,
          chunk_size,
          &add_to_target_table/2
        )
      end
    end

    :ok
  end

  def add_to_table({sid, bid} = sid_bid, batch, opts) when is_integer(sid) do
    chunk_size = Keyword.get(opts, :chunk_size, 100)
    no_get_tid = Keyword.get(opts, :no_get_tid, true)
    check_queue_size = Keyword.get(opts, :check_queue_size, true)
    startup_queue = {sid, bid, nil}

    reducer =
      if check_queue_size do
        fn
          {{_, _, nil}, _}, acc -> acc
          {_obj, count}, acc when count >= @max_queue_size -> acc
          {obj, _count}, acc -> [obj | acc]
        end
      else
        fn
          {{_, _, nil}, _}, acc -> acc
          {obj, _count}, acc -> [obj | acc]
        end
      end

    if no_get_tid do
      with all = [_ | _] <- list_counts_with_tids(sid_bid),
           available_queues = [_ | _] <- Enum.reduce(all, [], reducer) do
        Logflare.Utils.chunked_round_robin(
          batch,
          available_queues,
          chunk_size,
          &add_to_target_table/2
        )
      else
        _ ->
          add_to_table(startup_queue, batch)
      end
    else
      proc_counts =
        list_counts(sid_bid)
        |> Enum.sort_by(fn {_key, count} -> count end, :asc)
        |> Enum.filter(fn
          {{_, _, nil}, _} -> false
          {{_, _, _}, _} -> true
        end)

      procs = Enum.map(proc_counts, fn {key, _count} -> key end)

      if procs == [] do
        add_to_table({sid, bid, nil}, batch)
      else
        Logflare.Utils.chunked_round_robin(
          batch,
          procs,
          chunk_size,
          &add_to_target_table/2
        )
      end
    end

    :ok
  end

  def add_to_table({sid_bid_pid, tid}, batch, _opts) when is_tuple(sid_bid_pid) do
    insert_pointer_batch(sid_bid_pid, tid, batch)
  end

  def add_to_table({_, _, _} = sid_bid_pid, batch, _opts) do
    get_tid(sid_bid_pid)
    |> case do
      nil ->
        {:error, :not_initialized}

      tid ->
        add_to_table({sid_bid_pid, tid}, batch)
    end
  end

  # Claims the pointer row first — insert_new/2 into `queue_tid`, keyed by the event's
  # own id, is atomic per key, so a duplicate id already pending in this same queue is
  # rejected outright rather than overwriting (and thereby orphaning) whatever
  # generation-store row the existing pointer references. Only once the claim succeeds
  # is the event's full body written into the current generation's table (`gen_tid`),
  # under a freshly generated `gen_event_id` rather than the event's own id — this
  # decouples the shared store's key from the (possibly duplicated) event id, so
  # independent producers that each receive their own copy of the same id never collide
  # on the same generation-store row. Pointer row shape: {event_id, generation_tid,
  # gen_event_id, size, retries, event_type, day_bucket, ingest_freshness}.
  defp insert_pointer_batch(sid_bid_pid, queue_tid, batch) do
    queues_key = pointer_queues_key(sid_bid_pid)
    gen_tid = current_generation_tid(queues_key)

    for %{id: id} = event <- batch do
      gen_event_id = make_ref()

      row =
        {id, gen_tid, gen_event_id, :erlang.external_size(event.body), event.retries || 0,
         event.event_type, event.day_bucket, event.ingest_freshness}

      if :ets.insert_new(queue_tid, row) do
        :ets.insert(gen_tid, {gen_event_id, event})
      end
    end

    :ok
  rescue
    ArgumentError ->
      emit_stale_ets_table_telemetry()

      case sid_bid_pid do
        {_, _, nil} -> {:error, :not_initialized}
        _ -> add_to_table(put_elem(sid_bid_pid, 2, nil), batch)
      end
  end

  @spec pointer_queues_key(consolidated_table_key() | table_key() | spool_producer_table_key()) ::
          consolidated_queues_key() | queues_key() | spool_producer_queues_key()
  defp pointer_queues_key({:consolidated, bid, _pid}), do: {:consolidated, bid}
  defp pointer_queues_key({:spool_producer, nil, _pid}), do: {:spool_producer, nil}
  defp pointer_queues_key({sid, bid, _pid}), do: {sid, bid}

  defp add_to_target_table(chunk, target), do: add_to_table(target, chunk)

  @doc """
  Moves an entire queue from an origin to a target.

  Claims each row from `from_tid` via `:ets.take/2` before inserting it into `to_tid`,
  rather than copying the row `:ets.foldl/3` handed us and only then deleting it — a
  live producer can independently drain the same `from_tid` (e.g. the startup queue,
  during another producer's own scale-up) via its own `pop_pending_pointers/2` claim.
  Taking first means whichever side's `:ets.take/2` actually wins is the only one that
  ever sees the row again: if the other producer already claimed it, ours comes back
  empty and we skip it, instead of recreating an already-claimed pointer in `to_tid`
  and risking both sides processing (and acking) the same event
  (github.com/Logflare/logflare/pull/3690#discussion_r3598370787).
  """
  def move(from, to) when is_tuple(from) and is_tuple(to) do
    with from_tid when from_tid != nil <- get_tid(from),
         to_tid when to_tid != nil <- get_tid(to) do
      moved =
        :ets.foldl(
          fn {id, _, _, _, _, _, _, _}, acc -> acc + take_and_insert(from_tid, to_tid, id) end,
          0,
          from_tid
        )

      {:ok, moved}
    else
      nil -> {:error, :not_initialized}
    end
  end

  defp take_and_insert(from_tid, to_tid, id) do
    case :ets.take(from_tid, id) do
      [row] ->
        :ets.insert(to_tid, row)
        1

      [] ->
        0
    end
  end

  @doc """
  Deletes the queue associated with the given source-backend-pid.
  """
  @spec delete_queue(source_backend_pid()) :: :ok | {:error, :not_initialized}
  def delete_queue({sid, bid, pid} = sid_bid_pid) do
    with tid when tid != nil <- get_tid(sid_bid_pid) do
      :ets.delete(tid)
      :ets.delete_object(@ets_table_mapper, {{sid, bid}, pid, tid})
      :ok
    else
      nil -> {:error, :not_initialized}
    end
  end

  @doc """
  Returns a list of two-element tuples.
  First element is the table key.
  Second element is the pending count.
  """
  @spec list_pending_counts(queues_key() | consolidated_queues_key()) ::
          [{table_key() | consolidated_table_key(), non_neg_integer()}]
  def list_pending_counts(key) do
    traverse_queues(
      key,
      fn objs, acc ->
        items =
          for {sid_bid_pid, _tid} <- objs,
              count = total_pending(sid_bid_pid),
              is_integer(count) do
            {sid_bid_pid, count}
          end

        items ++ acc
      end,
      []
    )
  end

  @doc """
  Returns a list of two-element tuples.
  First element is table key.
  Second element is the size of the table.
  """
  @spec list_counts(queues_key() | consolidated_queues_key()) ::
          [{table_key() | consolidated_table_key(), non_neg_integer()}]
  def list_counts(key) do
    for {table_key, tid} <- list_queues_with_tids(key),
        size = :ets.info(tid, :size),
        is_integer(size) do
      {table_key, size}
    end
  end

  @spec list_counts_with_tids(
          queues_key()
          | consolidated_queues_key()
          | spool_producer_queues_key()
        ) ::
          [
            {table_key() | consolidated_table_key() | spool_producer_table_key(),
             non_neg_integer()}
          ]
  def list_counts_with_tids(key) do
    for {table_key, tid} <- list_queues_with_tids(key),
        size = :ets.info(tid, :size),
        is_integer(size) do
      {table_key, size}
    end
  end

  @doc """
  Counts pending items from a given table. Pointer rows are pending by construction
  (claiming one deletes it — see `pop_pending_pointers/2`), so this is just the
  table's size — O(1), no scan.
  """
  @spec total_pending(source_backend()) :: integer()
  @spec total_pending(source_backend_pid()) :: integer() | {:error, :not_initialized}
  def total_pending({_, _} = sid_bid) do
    # iterate over each matching source-backend queue and sum the totals
    for {_sid_bid_pid, count} <- list_pending_counts(sid_bid), reduce: 0 do
      acc -> acc + count
    end
  end

  def total_pending({_sid, _bid, _pid} = sid_bid_pid) do
    case get_tid(sid_bid_pid) do
      nil -> {:error, :not_initialized}
      tid -> table_size(tid)
    end
  end

  defp table_size(tid) do
    case :ets.info(tid, :size) do
      n when is_integer(n) -> n
      _ -> 0
    end
  end

  @doc """
  Returns the total count of events with a given status across all queues for a
  source-backend. Pointer rows have no status field — everything present is `:pending`
  by construction, so `:processing`/`:ingested` are always 0.
  """
  @spec total_by_status(
          queues_key() | spool_producer_queues_key(),
          :pending | :processing | :ingested
        ) ::
          non_neg_integer()
  def total_by_status(_key, status) when status in [:processing, :ingested], do: 0

  def total_by_status({_, _} = sid_bid, :pending) do
    traverse_queues(
      sid_bid,
      fn objs, acc ->
        Enum.reduce(objs, acc, fn {_sid_bid_pid, tid}, c -> c + table_size(tid) end)
      end,
      0
    )
  end

  @doc """
  Takes up to `n` pending pointers from a table, claiming each by removing its row
  outright — an atomic `:ets.take/2` per candidate, so a candidate raced away by another
  claimer between the select and the take simply comes back empty and is skipped. The
  returned pointer is built from what `take/2` actually removed, not the earlier
  `select/3` snapshot — if the same id was reclaimed and reinserted (fresh generation,
  bumped retries) in that narrow window, this still reflects its current row instead of
  stale data.

  Returns `LogEventPointer` structs: intended for ID-passing pipeline variants
  (ClickHouse, BigQuery, spool producer) to avoid copying full events through Broadway —
  the full event is resolved lazily via `lookup_event/2` (using the pointer's `tid`) only
  when actually needed, e.g. at batch-insert time.
  """
  @spec pop_pending_pointers(
          source_backend_pid() | spool_producer_table_key() | consolidated_table_key(),
          integer()
        ) ::
          {:ok, [LogEventPointer.t()], :ets.tid() | nil} | {:error, :not_initialized}
  def pop_pending_pointers(_, 0), do: {:ok, [], nil}

  def pop_pending_pointers(sid_bid_pid, n) when is_integer(n) do
    ms = [
      {{:"$1", :"$2", :"$3", :"$4", :"$5", :"$6", :"$7", :"$8"}, [],
       [{{:"$1", :"$2", :"$3", :"$4", :"$5", :"$6", :"$7", :"$8"}}]}
    ]

    pop_selected_pointers(sid_bid_pid, n, ms)
  end

  @doc """
  Counts pending pointers in one queue by the exact Broadway batch identity used by
  the ClickHouse pipeline: `{ingest_freshness, event_type, day_bucket}`.

  This scans pointer metadata only; full events remain in the generation store.
  """
  @spec pending_batch_key_counts(
          source_backend_pid()
          | spool_producer_table_key()
          | consolidated_table_key()
        ) :: %{optional(pointer_batch_key()) => non_neg_integer()}
  def pending_batch_key_counts(sid_bid_pid) do
    case get_tid(sid_bid_pid) do
      nil ->
        %{}

      tid ->
        tid
        |> :ets.select(@pointer_batch_key_match_spec)
        |> Enum.frequencies()
    end
  rescue
    ArgumentError ->
      emit_stale_ets_table_telemetry()
      %{}
  end

  @doc """
  Atomically claims up to `n` pointers for one ClickHouse batch key.

  As with `pop_pending_pointers/2`, the select is only a candidate snapshot. Each
  pointer is returned only when the following `:ets.take/2` wins its claim.
  """
  @spec pop_pending_pointers_by_batch_key(
          source_backend_pid() | spool_producer_table_key() | consolidated_table_key(),
          pointer_batch_key(),
          non_neg_integer()
        ) ::
          {:ok, [LogEventPointer.t()], :ets.tid() | nil} | {:error, :not_initialized}
  def pop_pending_pointers_by_batch_key(_sid_bid_pid, _batch_key, 0),
    do: {:ok, [], nil}

  def pop_pending_pointers_by_batch_key(
        sid_bid_pid,
        {freshness, event_type, day_bucket},
        n
      )
      when freshness in [:fresh, :stale] and
             event_type in [:log, :metric, :trace] and is_integer(day_bucket) and
             is_integer(n) and n > 0 do
    ms = [
      {{:"$1", :"$2", :"$3", :"$4", :"$5", event_type, day_bucket, freshness}, [],
       [{{:"$1", :"$2", :"$3", :"$4", :"$5", event_type, day_bucket, freshness}}]}
    ]

    pop_selected_pointers(sid_bid_pid, n, ms)
  end

  defp pop_selected_pointers(sid_bid_pid, n, ms) do
    with tid when tid != nil <- get_tid(sid_bid_pid),
         {selected, _cont} <- :ets.select(tid, ms, n) do
      confirmed =
        Enum.flat_map(selected, fn {id, _, _, _, _, _, _, _} -> take_pointer(tid, id) end)

      {:ok, confirmed, tid}
    else
      nil -> {:error, :not_initialized}
      :"$end_of_table" -> {:ok, [], nil}
    end
  end

  defp take_pointer(tid, id) do
    case :ets.take(tid, id) do
      [{^id, gen_tid, gen_event_id, size, retries, event_type, day_bucket, freshness}] ->
        [
          %LogEventPointer{
            id: id,
            tid: gen_tid,
            gen_event_id: gen_event_id,
            queue_tid: tid,
            size: size,
            retries: retries,
            event_type: event_type,
            day_bucket: day_bucket,
            ingest_freshness: freshness
          }
        ]

      [] ->
        []
    end
  end

  @doc """
  Re-inserts a previously-claimed pointer directly into the queue it was claimed from
  (`pointer.queue_tid`) — no round-robin redistribution, since that producer just proved
  itself alive by claiming this in the first place. Bump `pointer.retries` before
  calling this if the caller is retrying after a failure.

  If `queue_tid` has since gone stale (its owning producer died), the retry is dropped —
  same stale-table telemetry/rescue pattern used everywhere else in this module.
  """
  @spec reinsert_pointer(LogEventPointer.t()) :: :ok
  def reinsert_pointer(%LogEventPointer{} = pointer) do
    row =
      {pointer.id, pointer.tid, pointer.gen_event_id, pointer.size, pointer.retries,
       pointer.event_type, pointer.day_bucket, pointer.ingest_freshness}

    :ets.insert(pointer.queue_tid, row)
    :ok
  rescue
    ArgumentError ->
      emit_stale_ets_table_telemetry()
      :ok
  end

  @doc """
  Pops pending events from a given table, resolved to full `LogEvent`s, removing both
  the pointer and its underlying event immediately — unlike the claim-then-ack flow
  `pop_pending_pointers/2` uses, nothing is left for generation rotation to reclaim
  later. Use this for consolidated queues where events should be re-added on failure.
  """
  @spec pop_pending(table_key() | consolidated_table_key(), integer()) ::
          {:ok, [LogEvent.t()]} | {:error, :not_initialized}
  def pop_pending(_, 0), do: {:ok, []}

  def pop_pending(sid_bid_pid, n) when is_integer(n) do
    ms = [{{:"$1", :"$2", :"$3", :_, :_, :_, :_, :_}, [], [{{:"$1", :"$2", :"$3"}}]}]

    with tid when tid != nil <- get_tid(sid_bid_pid),
         size when is_integer(size) <- :ets.info(tid, :size),
         {selected, _cont} <- :ets.select(tid, ms, min(n, max(size, 1))) do
      events =
        selected
        |> Enum.map(&resolve_and_delete_pending(tid, &1))
        |> Enum.reject(&is_nil/1)

      {:ok, events}
    else
      nil -> {:error, :not_initialized}
      :"$end_of_table" -> {:ok, []}
    end
  end

  defp resolve_and_delete_pending(tid, {id, gen_tid, gen_event_id}) do
    :ets.delete(tid, id)

    case :ets.take(gen_tid, gen_event_id) do
      [{^gen_event_id, event}] -> event
      [] -> nil
    end
  rescue
    ArgumentError ->
      # The pointer's generation was already dropped by GenerationJanitor — expiry
      # deliberately leaves pending pointers behind rather than scanning to clean them
      # up, so a pointer can outlive its own generation. Same "not found" handling as
      # lookup_event/2's stale-generation case: the caller (pop_pending/2) already
      # rejects nil results, so this is skipped rather than crashing the producer.
      emit_stale_ets_table_telemetry()
      nil
  end

  @doc """
  Fetches up to `n` recent events for a `{sid, bid}`/consolidated queues_key, resolved
  to full `LogEvent`s. Read-only, doesn't claim anything.

  Merges two sources: the shared generation store directly (rather than iterating
  per-pid pointer tables — the store isn't per-pid, so that would return duplicates,
  and would miss anything already claimed: a claimed pointer is gone, but the event is
  still in the generation store until rotation reclaims it), and the recent-events
  cache (`list_recent_events/2` — BigQuery's ack keeps a copy here for events it's
  already deleted from the generation store, so they'd otherwise be invisible once
  acked; see `record_recent_event/2`). An event only ever lives in one of the two at a
  time, so no de-duplication is needed.

  This is what `list_recent_logs_local/2` uses.
  """
  @spec fetch_events(queues_key() | consolidated_queues_key(), integer()) ::
          {:ok, [LogEvent.t()]}
  def fetch_events(sid_bid, n) when is_integer(n) do
    from_generations =
      sid_bid
      |> list_generations()
      |> Enum.flat_map(fn {tid, _created_at} -> select_generation_events(tid, n) end)

    {:ok, from_generations ++ list_recent_events(sid_bid, n)}
  end

  defp select_generation_events(tid, n) do
    ms = [{{:_, :"$1"}, [], [:"$1"]}]

    with size when is_integer(size) <- :ets.info(tid, :size),
         {events, _cont} <- :ets.select(tid, ms, min(n, max(size, 1))) do
      events
    else
      _ -> []
    end
  end

  @doc """
  Truncates a given table
  """
  @spec truncate_table(
          source_backend_pid() | consolidated_table_key(),
          :all | :pending,
          integer()
        ) ::
          :ok | {:error, :not_initialized}

  def truncate_table({:consolidated, _bid, _pid} = key, status, n)
      when status in [:all, :pending],
      do: truncate_tid(get_tid(key), status, n)

  def truncate_table({sid, _bid, _pid} = key, status, n)
      when is_integer(sid) and status in [:all, :pending],
      do: truncate_tid(get_tid(key), status, n)

  @doc """
  Truncates a queue by its already-resolved `tid`, tolerating a stale (deleted) table.

  Truncates the table down to at most `n` rows kept (whichever `:ets.select/3` happens
  to return first, not necessarily oldest-first).

  This is the tid-based counterpart to `truncate_table/3`, which resolves the tid via
  `get_tid/1` first. The owning producer can die and ETS can reclaim its table between
  that resolution and the operations here; in that case the ETS calls raise
  `ArgumentError` (or `:ets.info/2` returns `:undefined`). Both are treated as a gone
  queue: `:stale_table` telemetry is emitted and `{:error, :not_initialized}` is returned
  instead of crashing the caller.
  """
  @spec truncate_tid(:ets.tid() | nil, :all | :pending, integer()) ::
          :ok | {:error, :not_initialized}
  def truncate_tid(nil, status, _n) when status in [:all, :pending],
    do: {:error, :not_initialized}

  def truncate_tid(tid, status, 0) when status in [:all, :pending] do
    :ets.delete_all_objects(tid)
    :ok
  rescue
    ArgumentError ->
      emit_stale_ets_table_telemetry()
      {:error, :not_initialized}
  end

  def truncate_tid(tid, status, n) when status in [:all, :pending] do
    ms = [{{:"$1", :_, :_, :_, :_, :_, :_, :_}, [], [:"$_"]}]

    with size when is_integer(size) <- :ets.info(tid, :size) do
      to_keep = select_to_insert(tid, ms, size, n)
      :ets.delete_all_objects(tid)
      :ets.insert(tid, to_keep)
      :ok
    else
      # :undefined from :ets.info/2 when the table was reclaimed before the
      # size read.
      _ -> {:error, :not_initialized}
    end
  rescue
    ArgumentError ->
      emit_stale_ets_table_telemetry()
      {:error, :not_initialized}
  end

  @doc """
  Safely deletes a single pointer row by id from a tid, ignoring stale (deleted) tables.
  Emits telemetry if the table no longer exists.

  Never touches the underlying event in the generation store — that's reclaimed only by
  generation rotation (see `drop_generation/2`).
  """
  @spec delete_id(:ets.tid(), term()) :: :ok
  def delete_id(tid, id) do
    :ets.delete(tid, id)
    :ok
  rescue
    ArgumentError ->
      emit_stale_ets_table_telemetry()
      :ok
  end

  defp select_to_insert(_tid, _ms, _size, 0), do: []

  defp select_to_insert(tid, ms, size, n) do
    case :ets.select(tid, ms, min(n, max(size, 1))) do
      {taken, _} -> taken
      :"$end_of_table" -> []
    end
  end

  @doc """
  Deletes a specific event from the table.
  If already deleted, it is a :noop.
  """
  @spec delete(source_backend_pid() | queues_key(), LogEvent.t()) ::
          :ok | :noop | {:error, :not_initialized}

  def delete({_, _} = sid_bid, %LogEvent{id: id}) do
    traverse_queues(sid_bid, fn objs, acc ->
      for {_sid_bid_pid, tid} <- objs do
        :ets.delete(tid, id)
      end

      acc
    end)

    :ok
  end

  def delete({_, _, _pid} = sid_bid_pid, %LogEvent{id: id}) do
    tid = get_tid(sid_bid_pid)

    if tid != nil do
      :ets.delete(tid, id)

      :ok
    else
      {:error, :not_initialized}
    end
  end

  @doc """
  Deletes multiple events from the table.
  """
  @spec delete_batch(
          source_backend_pid()
          | queues_key()
          | consolidated_queues_key()
          | spool_producer_queues_key(),
          [LogEvent.t()]
        ) :: :ok
  def delete_batch(_sid_bid, []), do: :ok

  def delete_batch({_, _} = sid_bid, events) when is_list(events) do
    ids = Enum.map(events, & &1.id)

    traverse_queues(sid_bid, fn objs, acc ->
      for {_sid_bid_pid, tid} <- objs, id <- ids do
        :ets.delete(tid, id)
      end

      acc
    end)

    :ok
  end

  @doc """
  Drops up to `n` pending pointer rows from the ingest event table, deleting each
  dropped pointer's underlying generation-store event too. Pointer rows have no status
  field — everything present is pending by construction, so there's nothing else to
  filter by.

  This is QueueJanitor's load-shedding path for a runaway buffer — deleting only the
  pointer row and leaving the event body behind in the generation store would miss the
  point: the memory it's meant to relieve wouldn't actually be freed until
  GenerationJanitor's next rotation (bounded by max_age_ms, up to a couple of minutes),
  not immediately as this is meant to do.
  """
  @spec drop_pending(source_backend_pid(), non_neg_integer()) :: :ok

  def drop_pending({_, _} = sid_bid, n) when is_integer(n) do
    traverse_queues(sid_bid, fn objs, acc ->
      num =
        for {sid_bid_pid, _tid} <- objs, reduce: 0 do
          acc ->
            {:ok, num} = drop_pending(sid_bid_pid, n)
            acc + num
        end

      num + acc
    end)

    :ok
  end

  @spec drop_pending(source_backend_pid() | consolidated_table_key(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | {:error, :not_initialized}
  def drop_pending({_, _, _} = sid_bid_pid, n) when is_integer(n) do
    ms = [{{:"$1", :"$2", :"$3", :_, :_, :_, :_, :_}, [], [{{:"$1", :"$2", :"$3"}}]}]

    with tid when tid != nil <- get_tid(sid_bid_pid),
         {taken, _cont} <- :ets.select(tid, ms, n) do
      for {id, gen_tid, gen_event_id} <- taken do
        :ets.delete(tid, id)
        delete_id(gen_tid, gen_event_id)
      end

      {:ok, Enum.count(taken)}
    else
      nil -> {:error, :not_initialized}
      :"$end_of_table" -> {:ok, 0}
    end
  end

  @doc """
  Deletes all mappings int he IngestEventQueue mapping table.
  """
  @spec delete_all_mappings() :: :ok
  def delete_all_mappings do
    :ets.delete_all_objects(@ets_table_mapper)
    :ok
  end

  @doc """
  Deletes all stale mappings in the IngestEventQueue table mapping
  """
  @spec delete_stale_mappings() :: :ok
  def delete_stale_mappings do
    :ets.safe_fixtable(@ets_table_mapper, true)

    res =
      :ets.match_object(@ets_table_mapper, :"$1", 100)
      |> next_and_cleanup()

    :ets.safe_fixtable(@ets_table_mapper, false)
    res
  end

  defp next_and_cleanup(:"$end_of_table"), do: :ok

  defp next_and_cleanup({to_check, cont}) do
    for {{sid, bid}, pid, tid} <- to_check do
      if :ets.info(tid, :name) == :undefined do
        :ets.delete_object(@ets_table_mapper, {{sid, bid}, pid, tid})
      end
    end

    :ets.match_object(cont)
    |> next_and_cleanup()
  end

  @doc """
  Select queues by source-backend combination or consolidated queue.
  """
  @spec list_queues(queues_key() | consolidated_queues_key() | spool_producer_queues_key()) ::
          [table_key() | consolidated_table_key() | spool_producer_table_key()]
  def list_queues({:consolidated, bid}) when is_integer(bid) do
    ms =
      Ex2ms.fun do
        {{:consolidated, ^bid}, pid, _tid} -> {:consolidated, ^bid, pid}
      end

    with {queues, _cont} <- :ets.select(@ets_table_mapper, ms, 1000) do
      queues
    else
      :"$end_of_table" -> []
    end
  end

  def list_queues({sid, bid}) do
    ms =
      Ex2ms.fun do
        {{^sid, ^bid}, pid, _tid} -> {^sid, ^bid, pid}
      end

    with {queues, _cont} <- :ets.select(@ets_table_mapper, ms, 1000) do
      queues
    else
      :"$end_of_table" -> []
    end
  end

  @doc """
  Select queues by source-backend combination or consolidated queue with their :ets.tid().
  """
  @spec list_queues_with_tids(
          queues_key()
          | consolidated_queues_key()
          | spool_producer_queues_key()
        ) ::
          [{table_key() | consolidated_table_key() | spool_producer_table_key(), :ets.tid()}]
  def list_queues_with_tids({:consolidated, bid}) when is_integer(bid) do
    ms =
      Ex2ms.fun do
        {{:consolidated, ^bid}, pid, tid} -> {{:consolidated, ^bid, pid}, tid}
      end

    with {queues, _cont} <- :ets.select(@ets_table_mapper, ms, 1000) do
      queues
    else
      :"$end_of_table" -> []
    end
  end

  def list_queues_with_tids({sid, bid}) do
    ms =
      Ex2ms.fun do
        {{^sid, ^bid}, pid, tid} -> {{^sid, ^bid, pid}, tid}
      end

    with {queues, _cont} <- :ets.select(@ets_table_mapper, ms, 1000) do
      queues
    else
      :"$end_of_table" -> []
    end
  end

  @doc """
  Performs a reduce across all queues of a source-backend combination or consolidated queue.

  Startup queue is included.

  """
  def traverse_queues(key, func, acc \\ nil, opts \\ [])

  def traverse_queues({:consolidated, bid}, func, acc, _opts) when is_integer(bid) do
    :ets.safe_fixtable(@ets_table_mapper, true)

    ms =
      Ex2ms.fun do
        {{:consolidated, ^bid}, pid, tid} -> {{:consolidated, ^bid, pid}, tid}
      end

    res =
      :ets.select(@ets_table_mapper, ms, 250)
      |> select_traverse(func, acc)

    :ets.safe_fixtable(@ets_table_mapper, false)
    res
  end

  def traverse_queues({sid, bid}, func, acc, _opts) do
    :ets.safe_fixtable(@ets_table_mapper, true)

    ms =
      Ex2ms.fun do
        {{^sid, ^bid}, pid, tid} -> {{^sid, ^bid, pid}, tid}
      end

    res =
      :ets.select(@ets_table_mapper, ms, 250)
      |> select_traverse(func, acc)

    :ets.safe_fixtable(@ets_table_mapper, false)
    res
  end

  defp select_traverse(res, func, acc)

  defp select_traverse(:"$end_of_table", _func, acc) do
    acc
  end

  defp select_traverse({selected, cont}, func, acc) do
    case func.(selected, acc) do
      {:stop, acc} ->
        acc

      acc ->
        :ets.select(cont)
        |> select_traverse(func, acc)
    end
  end

  @spec emit_stale_ets_table_telemetry() :: :ok
  defp emit_stale_ets_table_telemetry do
    :telemetry.execute([:logflare, :ingest_event_queue, :stale_table], %{count: 1}, %{})
  end
end
