defmodule Logflare.Backends.IngestEventQueue do
  @moduledoc """
  GenServer will manage the ETS buffer mapping and own that table.

  :ets-backed buffer uses an :ets mapping pattern to fan out multiple :ets tables.
  """
  use GenServer

  alias Logflare.Sources.Source
  alias Logflare.Backends.Backend
  alias Logflare.LogEvent

  require Ex2ms

  @ets_table_mapper :ingest_event_queue_mapping
  @ets_table :source_ingest_events
  @max_queue_size 30_000

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

  defguardp is_pid_or_nil(value) when is_pid(value) or is_nil(value)

  def max_queue_size, do: @max_queue_size

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

    {:ok, %{}}
  end

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

        :ets.insert(@ets_table_mapper, {{:consolidated, bid}, pid, tid})
        {:ok, tid}

      tid ->
        {:error, :already_exists, tid}
    end
  end

  def upsert_tid({:spool_producer, nil, pid} = key) when is_pid_or_nil(pid) do
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

        :ets.insert(@ets_table_mapper, {{:spool_producer, nil}, pid, tid})
        {:ok, tid}

      tid ->
        {:error, :already_exists, tid}
    end
  end

  def upsert_tid({sid, bid, pid} = sid_bid_pid)
      when is_integer(sid) and (is_integer(bid) or is_nil(bid)) and is_pid_or_nil(pid) do
    case get_tid(sid_bid_pid) do
      nil ->
        tid =
          :ets.new(@ets_table, [
            :public,
            :set,
            {:decentralized_counters, false},
            {:write_concurrency, :auto},
            {:read_concurrency, true}
          ])

        :ets.insert(@ets_table_mapper, {{sid, bid}, pid, tid})
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

  The record will be marked as :pending.
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
          {_obj, count}, acc when count >= @max_queue_size -> acc
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
    objects =
      for %{id: id} = event <- batch do
        # New/requeued rows start unclaimed (claim counter 0, claimed_at 0); see claim_pending/3.
        {id, :pending, event, :erlang.external_size(event.body), 0, 0}
      end

    try do
      :ets.insert(tid, objects)
      :ok
    rescue
      ArgumentError ->
        # The owning producer died and ETS reclaimed its table between tid
        # resolution and this insert. Re-route to the supervisor-owned startup
        # queue (where a clean producer exit also drains to) so the batch is not
        # lost; give up only if the startup queue itself is gone.
        emit_stale_ets_table_telemetry()

        case sid_bid_pid do
          {_, _, nil} -> {:error, :not_initialized}
          _ -> add_to_table(put_elem(sid_bid_pid, 2, nil), batch)
        end
    end
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

  defp add_to_target_table(chunk, target), do: add_to_table(target, chunk)

  @doc """
  Moves an entire queue from an origin to a target.
  """
  def move(from, to) when is_tuple(from) and is_tuple(to) do
    with from_tid when from_tid != nil <- get_tid(from),
         to_tid when to_tid != nil <- get_tid(to) do
      moved =
        :ets.foldl(
          fn el, acc ->
            :ets.insert(to_tid, el)
            :ets.delete_object(from_tid, el)
            acc + 1
          end,
          0,
          from_tid
        )

      {:ok, moved}
    else
      nil -> {:error, :not_initialized}
    end
  end

  @doc """
  Marks records as ingested
  """
  @spec mark_ingested(source_backend_pid(), [LogEvent.t()]) ::
          {:ok, non_neg_integer()} | {:error, :not_initialized}
  def mark_ingested(sid_bid_pid, events) do
    tid = get_tid(sid_bid_pid)

    if tid != nil do
      for event <- events do
        # Clear claimed_at on exit from :processing so it only carries a real stamp while in flight.
        :ets.update_element(tid, event.id, [{2, :ingested}, {6, 0}])
      end

      {:ok, Enum.count(events)}
    else
      {:error, :not_initialized}
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
  Counts pending items from a given table
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
    ms =
      Ex2ms.fun do
        {_event_id, :pending, _event, _size, _claim, _claimed_at} -> true
      end

    with tid when tid != nil <- get_tid(sid_bid_pid),
         num when is_integer(num) <- :ets.select_count(tid, ms) do
      num
    else
      nil -> {:error, :not_initialized}
    end
  end

  @doc """
  Returns the total count of events with a given status across all queues for a source-backend.
  """
  @spec total_by_status(
          queues_key() | spool_producer_queues_key(),
          :pending | :processing | :ingested
        ) ::
          non_neg_integer()
  def total_by_status({_, _} = sid_bid, status) do
    ms =
      Ex2ms.fun do
        {_event_id, event_status, _event, _size, _claim, _claimed_at}
        when event_status == ^status ->
          true
      end

    traverse_queues(
      sid_bid,
      fn objs, acc ->
        Enum.reduce(objs, acc, fn {_sid_bid_pid, tid}, c ->
          c + :ets.select_count(tid, ms)
        end)
      end,
      0
    )
  end

  @doc """
  Takes pending items from a given table
  """
  @spec take_pending(source_backend_pid(), integer()) ::
          {:ok, [LogEvent.t()]} | {:error, :not_initialized}
  def take_pending(_, 0), do: {:ok, []}

  def take_pending(sid_bid_pid, n) when is_integer(n) do
    ms =
      Ex2ms.fun do
        {_event_id, :pending, event, _size, _claim, _claimed_at} -> event
      end

    with tid when tid != nil <- get_tid(sid_bid_pid),
         size when is_integer(size) <- :ets.info(tid, :size),
         {taken, _cont} <- :ets.select(tid, ms, min(n, max(size, 1))) do
      {:ok, taken}
    else
      nil -> {:error, :not_initialized}
      :"$end_of_table" -> {:ok, []}
    end
  end

  @doc """
  Takes pending item IDs from a given table, marking them as `:processing` in-place.

  Returns `{:ok, ids, tid}` where `ids` is a list of event IDs and `tid` is the ETS
  table reference. Intended for use with the BigQuery pipeline to reduce data copying
  through Broadway stages — only pointers travel the pipeline; full events are fetched
  from ETS at batch-insert time.
  """
  @spec take_pending_ids(source_backend_pid() | spool_producer_table_key(), integer()) ::
          {:ok, [{term(), non_neg_integer()}], :ets.tid() | nil} | {:error, :not_initialized}
  def take_pending_ids(_, 0), do: {:ok, [], nil}

  def take_pending_ids(sid_bid_pid, n) when is_integer(n) do
    ms =
      Ex2ms.fun do
        {event_id, :pending, _event, size, _claim, _claimed_at} -> {event_id, size}
      end

    with tid when tid != nil <- get_tid(sid_bid_pid),
         {taken_pairs, _cont} <- :ets.select(tid, ms, n) do
      # One monotonic read per batch (not per event); every row claimed in this batch shares
      # the same claimed_at stamp, which is all the stale-recovery janitor needs.
      claimed_at = System.monotonic_time(:millisecond)

      # The claim counter's 0 -> 1 winner takes the event; a 2+ result (another consumer,
      # or a write_concurrency duplicate row within this select) loses, so no dedup needed.
      confirmed =
        Enum.filter(taken_pairs, fn {id, _size} -> claim_pending(tid, id, claimed_at) end)

      {:ok, confirmed, tid}
    else
      nil -> {:error, :not_initialized}
      :"$end_of_table" -> {:ok, [], nil}
    end
  end

  # Claim invariant:
  #   * every `:pending` row has `claim == 0` and `claimed_at == 0`
  #   * `update_counter` is the atomic election; the `:processing` write is a separate step
  #   * the winning claim stamps `claimed_at` with the batch's monotonic time so the
  #     QueueJanitor can recover rows stuck in `:processing` past a staleness threshold
  #   * any transition back to `:pending` must reset `claim` and `claimed_at` (add_to_table/3,
  #     the QueueJanitor reset, and update_status/3 all do)
  #   * id_passing and non-id_passing producers never share a pid-keyed queue, so the
  #     claim path and the mark_ingested/pop path never touch the same rows
  @spec claim_pending(:ets.tid(), term(), integer()) :: boolean()
  defp claim_pending(tid, id, claimed_at) do
    # Return update_element/3's result: it is false for a missing key, so a row deleted
    # between the counter bump and the status write is rejected rather than claimed.
    if :ets.update_counter(tid, id, {5, 1}) == 1 do
      :ets.update_element(tid, id, [{2, :processing}, {6, claimed_at}])
    else
      false
    end
  rescue
    # The row was deleted before the counter bump; update_counter raises on a missing key.
    ArgumentError -> false
  end

  @doc """
  Pops pending events from a given table, removing them atomically.

  Unlike `take_pending/2`, this function removes the events from the queue
  immediately rather than leaving them with a `:pending` status.
  Use this for consolidated queues where events should be re-added on failure.
  """
  @spec pop_pending(table_key() | consolidated_table_key(), integer()) ::
          {:ok, [LogEvent.t()]} | {:error, :not_initialized}
  def pop_pending(_, 0), do: {:ok, []}

  def pop_pending(sid_bid_pid, n) when is_integer(n) do
    select_ms =
      Ex2ms.fun do
        {event_id, :pending, event, _size, _claim, _claimed_at} -> {event_id, event}
      end

    with tid when tid != nil <- get_tid(sid_bid_pid),
         size when is_integer(size) <- :ets.info(tid, :size),
         {selected, _cont} <- :ets.select(tid, select_ms, min(n, max(size, 1))) do
      events =
        for {event_id, event} <- selected do
          :ets.delete(tid, event_id)
          event
        end

      {:ok, events}
    else
      nil -> {:error, :not_initialized}
      :"$end_of_table" -> {:ok, []}
    end
  end

  @spec fetch_events(source_backend_pid(), integer()) ::
          {:ok, [LogEvent.t()]} | {:error, :not_initialized}
  def fetch_events({_, _, _} = sid_bid_pid, n) do
    ms =
      Ex2ms.fun do
        {_event_id, _, event, _size, _claim, _claimed_at} -> event
      end

    with tid when tid != nil <- get_tid(sid_bid_pid),
         size when is_integer(size) <- :ets.info(tid, :size),
         {selected, _cont} <- :ets.select(tid, ms, min(n, max(size, 1))) do
      {:ok, selected}
    else
      nil -> {:error, :not_initialized}
      :"$end_of_table" -> {:ok, []}
    end
  end

  def fetch_events(sid_bid, n) when is_integer(n) do
    events =
      traverse_queues(
        sid_bid,
        fn objs, acc ->
          items =
            for {sid_bid_pid, _tid} <- objs do
              fetch_events_or_empty(sid_bid_pid, n)
            end
            |> List.flatten()

          items ++ acc
        end,
        []
      )

    {:ok, events}
  end

  defp fetch_events_or_empty(sid_bid_pid, n) do
    case fetch_events(sid_bid_pid, n) do
      {:ok, events} -> events
      _ -> []
    end
  end

  @doc """
  Truncates a given table
  """
  @spec truncate_table(
          source_backend_pid() | consolidated_table_key(),
          :all | :pending | :ingested,
          integer()
        ) ::
          :ok | {:error, :not_initialized}

  def truncate_table({:consolidated, _bid, _pid} = key, status, n)
      when status in [:all, :pending, :ingested],
      do: truncate_tid(get_tid(key), status, n)

  def truncate_table({sid, _bid, _pid} = key, status, n)
      when is_integer(sid) and status in [:all, :pending, :ingested],
      do: truncate_tid(get_tid(key), status, n)

  @doc """
  Truncates a queue by its already-resolved `tid`, tolerating a stale
  (deleted) table.

  This is the tid-based counterpart to `truncate_table/3`, which resolves the
  tid via `get_tid/1` first. The owning producer can die and ETS can reclaim
  its table between that resolution and the operations here; in that case the
  ETS calls raise `ArgumentError` (or `:ets.info/2` returns `:undefined`).
  Both are treated as a gone queue: `:stale_table` telemetry is emitted and
  `{:error, :not_initialized}` is returned instead of crashing the caller.
  """
  @spec truncate_tid(:ets.tid() | nil, :all | :pending | :ingested, integer()) ::
          :ok | {:error, :not_initialized}
  def truncate_tid(nil, status, _n) when status in [:all, :pending, :ingested],
    do: {:error, :not_initialized}

  def truncate_tid(tid, :all, 0) do
    :ets.delete_all_objects(tid)
    :ok
  rescue
    ArgumentError ->
      emit_stale_ets_table_telemetry()
      {:error, :not_initialized}
  end

  def truncate_tid(tid, status, n) when status in [:all, :pending, :ingested] do
    ms =
      Ex2ms.fun do
        {_event_id, _event_status, event, _size, _claim, _claimed_at} = obj
        when ^status == :all ->
          obj

        {_event_id, event_status, event, _size, _claim, _claimed_at} = obj
        when event_status == ^status ->
          obj
      end

    del_ms =
      Ex2ms.fun do
        {_event_id, _event_status, _event, _size, _claim, _claimed_at} = _obj
        when ^status == :all ->
          true

        {_event_id, event_status, _event, _size, _claim, _claimed_at} = _obj
        when event_status == ^status ->
          true
      end

    with size when is_integer(size) <- :ets.info(tid, :size) do
      to_insert = select_to_insert(tid, ms, size, n)
      :ets.select_delete(tid, del_ms)
      :ets.insert(tid, to_insert)
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
  Looks up a single event by id in a tid, returning its status, the event
  itself, and its byte size — hiding the row's internal representation
  (which also carries claim/claimed_at for the stale-processing recovery
  mechanism; see queue_janitor.ex) from callers that just want the event.
  """
  @spec lookup_id(:ets.tid(), term()) ::
          {term(), :pending | :processing | :ingested, LogEvent.t(), non_neg_integer()} | nil
  def lookup_id(tid, id) do
    case :ets.lookup(tid, id) do
      [{^id, status, event, byte_size, _claim, _claimed_at}] -> {id, status, event, byte_size}
      [] -> nil
    end
  end

  @doc """
  Safely deletes a single event by id from a tid, ignoring stale (deleted) tables.
  Emits telemetry if the table no longer exists.
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

  @doc """
  Safely updates the status of a single event in a tid, ignoring stale (deleted) tables.
  Emits telemetry if the table no longer exists.
  """
  @spec update_status(:ets.tid(), term(), :pending | :processing | :ingested) :: :ok
  def update_status(tid, id, status) do
    # Keep claim/claimed_at consistent with the target status in the same atomic update:
    #   * :pending — reset claim and claimed_at so the row is claimable and not seen as stale
    #   * :processing — stamp claimed_at so stale recovery can age the row out
    #   * :ingested — clear claimed_at; it only carries a real stamp while in flight
    update =
      case status do
        :pending -> [{2, :pending}, {5, 0}, {6, 0}]
        :processing -> [{2, :processing}, {6, System.monotonic_time(:millisecond)}]
        :ingested -> [{2, :ingested}, {6, 0}]
      end

    :ets.update_element(tid, id, update)
    :ok
  rescue
    ArgumentError ->
      emit_stale_ets_table_telemetry()
      :ok
  end

  @doc """
  Returns the list of event IDs currently marked as `:processing` in a queue.
  """
  @spec list_processing_ids(table_key() | consolidated_table_key()) :: [term()]
  def list_processing_ids(key) do
    ms =
      Ex2ms.fun do
        {id, :processing, _event, _size, _claim, _claimed_at} -> id
      end

    with tid when tid != nil <- get_tid(key),
         {ids, _cont} <- :ets.select(tid, ms, 10_000) do
      ids
    else
      _ -> []
    end
  end

  @doc """
  Returns up to `limit` IDs of `:processing` events claimed at or before `cutoff`.

  `cutoff` is a `System.monotonic_time(:millisecond)` value; rows whose `claimed_at`
  stamp is `<= cutoff` have been in flight longer than the staleness threshold. Status
  is pinned to `:processing` in the match head, so `:pending`/`:ingested` rows (which
  carry `claimed_at == 0`) are never returned regardless of the monotonic clock's sign.

  `limit` bounds the IDs returned, not necessarily the rows scanned: the table is not
  indexed by `claimed_at`, so ETS may walk further to find matching stale rows.

  Used by QueueJanitor to detect stale in-flight events.
  """
  @spec list_stale_processing_ids(
          table_key() | consolidated_table_key(),
          integer(),
          pos_integer()
        ) ::
          [term()]
  def list_stale_processing_ids(key, cutoff, limit) do
    ms =
      Ex2ms.fun do
        {id, :processing, _event, _size, _claim, claimed_at} when claimed_at <= ^cutoff -> id
      end

    with tid when tid != nil <- get_tid(key),
         {ids, _cont} <- :ets.select(tid, ms, limit) do
      ids
    else
      _ -> []
    end
  end

  @doc """
  Resets an exact stale `:processing` row back to `:pending` with `new_event`, clearing the
  claim counter and `claimed_at`.

  `expected` is the full row a caller previously observed via `:ets.lookup/2`. The match pins
  every field, so a row that was acked, deleted, or re-claimed (carrying a newer `claimed_at`)
  between observation and this call does not match and is left untouched. Returns `:reset` when
  the row was replaced, `:skip` otherwise.
  """
  @spec reset_stale_event(:ets.tid(), tuple(), LogEvent.t()) :: :reset | :skip
  def reset_stale_event(
        tid,
        {id, :processing, _event, size, _claim, _claimed_at} = expected,
        new_event
      ) do
    case :ets.select_replace(tid, [
           {expected, [], [{:const, {id, :pending, new_event, size, 0, 0}}]}
         ]) do
      1 -> :reset
      0 -> :skip
    end
  end

  @doc """
  Deletes an exact stale `:processing` row.

  `expected` is the full row a caller previously observed; the match pins every field (see
  `reset_stale_event/3`), so a row changed between observation and this call is left untouched.
  Returns `:drop` when the row was deleted, `:skip` otherwise.
  """
  @spec drop_stale_event(:ets.tid(), tuple()) :: :drop | :skip
  def drop_stale_event(tid, {_id, :processing, _event, _size, _claim, _claimed_at} = expected) do
    case :ets.select_delete(tid, [{expected, [], [true]}]) do
      1 -> :drop
      0 -> :skip
    end
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
  Drop events from the ingest event table.
  """
  @spec drop(source_backend_pid(), :all | :pending | :ingested, non_neg_integer()) :: :ok

  def drop({_, _} = sid_bid, filter, n)
      when is_integer(n) and filter in [:pending, :all, :ingested] do
    traverse_queues(sid_bid, fn objs, acc ->
      num =
        for {sid_bid_pid, _tid} <- objs, reduce: 0 do
          acc ->
            {:ok, num} = drop(sid_bid_pid, filter, n)
            acc + num
        end

      num + acc
    end)

    :ok
  end

  @spec drop(
          source_backend_pid() | consolidated_table_key(),
          :all | :pending | :ingested,
          non_neg_integer()
        ) ::
          {:ok, non_neg_integer()} | {:error, :not_initialized}
  def drop({_, _, _} = sid_bid_pid, filter, n)
      when is_integer(n) and filter in [:pending, :all, :ingested] do
    # chunk over table and drop
    ms =
      Ex2ms.fun do
        {event_id, _status, _event, _size, _claim, _claimed_at} = _obj when ^filter == :all ->
          event_id

        {event_id, status, _event, _size, _claim, _claimed_at} = _obj when status == ^filter ->
          event_id
      end

    with tid when tid != nil <- get_tid(sid_bid_pid),
         {taken, _cont} <- :ets.select(tid, ms, n) do
      for key <- taken do
        :ets.delete(tid, key)
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
  @spec list_queues(queues_key() | consolidated_queues_key()) ::
          [table_key() | consolidated_table_key()]
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
