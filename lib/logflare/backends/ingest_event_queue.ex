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
  @spec get_tid(table_key() | consolidated_table_key()) :: :ets.tid() | nil
  def get_tid({:consolidated, bid, pid}) when is_integer(bid) do
    :ets.match(@ets_table_mapper, {{:consolidated, bid}, pid, :"$1"}, 1)
    |> then(fn
      {[[tid]], _cont} ->
        if :ets.info(tid) != :undefined, do: tid

      _ ->
        nil
    end)
  end

  def get_tid({sid, bid, pid}) do
    :ets.match(@ets_table_mapper, {{sid, bid}, pid, :"$1"}, 1)
    |> then(fn
      {[[tid]], _cont} ->
        if :ets.info(tid) != :undefined, do: tid

      _ ->
        nil
    end)
  end

  @doc """
  Creates or updates a private :ets table. The :ets table mapper is stored in #{@ets_table_mapper} .
  """
  @spec upsert_tid(table_key() | consolidated_table_key()) ::
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
          | consolidated_table_key(),
          [LogEvent.t()],
          Keyword.t()
        ) :: :ok | {:error, :not_initialized}
  def add_to_table(sid_bid_or_sid_bid_pid, batch, opts \\ [])

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
          fn chunk, target ->
            add_to_table(target, chunk)
          end
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

      chunking_func = fn chunk, target ->
        add_to_table(target, chunk)
      end

      if procs == [] do
        add_to_table({:consolidated, bid, nil}, batch)
      else
        Logflare.Utils.chunked_round_robin(
          batch,
          procs,
          chunk_size,
          chunking_func
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
          fn chunk, target ->
            add_to_table(target, chunk)
          end
        )
      else
        _ ->
          # no available queues, add to startup queue
          add_to_table(startup_queue, batch)
      end
    else
      proc_counts =
        list_counts(sid_bid)
        |> Enum.sort_by(fn {_key, count} -> count end, :asc)
        |> Enum.filter(fn
          # exclude startup queue
          {{_, _, nil}, _} -> false
          {{_, _, _}, _} -> true
        end)

      procs = Enum.map(proc_counts, fn {key, _count} -> key end)

      chunking_func = fn chunk, target ->
        add_to_table(target, chunk)
      end

      if procs == [] do
        # not yet started, add to startup queue
        add_to_table({sid, bid, nil}, batch)
      else
        Logflare.Utils.chunked_round_robin(
          batch,
          procs,
          chunk_size,
          chunking_func
        )
      end
    end

    :ok
  end

  def add_to_table({sid_bid_pid, tid}, batch, _opts) when is_tuple(sid_bid_pid) do
    objects =
      for %{id: id} = event <- batch do
        {id, :pending, event}
      end

    :ets.insert(tid, objects)
    :ok
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
        :ets.update_element(tid, event.id, {2, :ingested})
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

  @spec list_counts_with_tids(queues_key() | consolidated_queues_key()) ::
          [{table_key() | consolidated_table_key(), non_neg_integer()}]
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
        {_event_id, :pending, _event} -> true
      end

    with tid when tid != nil <- get_tid(sid_bid_pid),
         num when is_integer(num) <- :ets.select_count(tid, ms) do
      num
    else
      nil -> {:error, :not_initialized}
    end
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
        {_event_id, :pending, event} -> event
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
        {event_id, :pending, event} -> {event_id, event}
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
        {_event_id, _, event} -> event
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
              case fetch_events(sid_bid_pid, n) do
                {:ok, events} -> events
                _ -> []
              end
            end
            |> List.flatten()

          items ++ acc
        end,
        []
      )

    {:ok, events}
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
      do: do_truncate_table(key, status, n)

  def truncate_table({sid, _bid, _pid} = key, status, n)
      when is_integer(sid) and status in [:all, :pending, :ingested],
      do: do_truncate_table(key, status, n)

  defp do_truncate_table(key, :all, 0) do
    with tid when tid != nil <- get_tid(key) do
      :ets.delete_all_objects(tid)
      :ok
    else
      nil -> {:error, :not_initialized}
    end
  end

  defp do_truncate_table(key, status, n) do
    ms =
      Ex2ms.fun do
        {_event_id, _event_status, event} = obj when ^status == :all -> obj
        {_event_id, event_status, event} = obj when event_status == ^status -> obj
      end

    del_ms =
      Ex2ms.fun do
        {_event_id, _event_status, event} = obj when ^status == :all -> true
        {_event_id, event_status, event} = obj when event_status == ^status -> true
      end

    with tid when tid != nil <- get_tid(key),
         size when is_integer(size) <- :ets.info(tid, :size) do
      to_insert =
        if n == 0 do
          []
        else
          :ets.select(tid, ms, min(n, max(size, 1)))
          |> case do
            {taken, _} -> taken
            :"$end_of_table" -> []
          end
        end

      :ets.select_delete(tid, del_ms)
      :ets.insert(tid, to_insert)
      :ok
    else
      nil -> {:error, :not_initialized}
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
  @spec delete_batch(source_backend_pid() | queues_key() | consolidated_queues_key(), [
          LogEvent.t()
        ]) :: :ok
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
        {event_id, _status, _event} = obj when ^filter == :all -> event_id
        {event_id, status, _event} = obj when status == ^filter -> event_id
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
      if :ets.info(tid) == :undefined do
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
  @spec list_queues_with_tids(queues_key() | consolidated_queues_key()) ::
          [{table_key() | consolidated_table_key(), :ets.tid()}]
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
end
