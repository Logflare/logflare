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
  @type source_backend_pid ::
          {Source.t() | pos_integer(), Backend.t() | pos_integer() | nil, pid() | nil}
  @type table_key :: {pos_integer(), pos_integer() | nil, pid() | nil}
  @type queues_key :: {pos_integer(), pos_integer() | nil}

  ## Server
  def start_link(_args) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__, hibernate_after: 1_000)
  end

  @impl GenServer
  def init(_args) do
    :ets.new(@ets_table_mapper, [
      :public,
      :named_table,
      :set,
      {:write_concurrency, false},
      {:read_concurrency, false},
      {:decentralized_counters, false}
    ])

    {:ok, %{}}
  end

  @doc """
  Retrieves a private tid of a given source-backend combination.
  """
  @spec get_tid(table_key()) :: :ets.tid() | nil
  def get_tid({sid, bid, pid}) do
    :ets.lookup_element(@ets_table_mapper, {sid, bid, pid}, 2, nil)
    # staleness check
    |> then(fn
      nil ->
        nil

      tid ->
        if :ets.info(tid) != :undefined, do: tid
    end)
  end

  @doc """
  Creates or updates a private :ets table. The :ets table mapper is stored in #{@ets_table_mapper} .
  """
  @spec upsert_tid(table_key()) :: {:ok, :ets.tid()} | {:error, :already_exists, :ets.tid()}
  def upsert_tid({sid, bid, pid} = sid_bid_pid)
      when is_integer(sid) and (is_integer(bid) or is_nil(bid)) and (is_pid(pid) or is_nil(pid)) do
    case get_tid(sid_bid_pid) do
      nil ->
        # create and insert
        tid =
          :ets.new(@ets_table, [
            :public,
            :set,
            {:decentralized_counters, false},
            {:write_concurrency, true},
            {:read_concurrency, true}
          ])

        :ets.insert(@ets_table_mapper, {sid_bid_pid, tid})
        {:ok, tid}

      tid ->
        {:error, :already_exists, tid}
    end
  end

  @doc """
  Retrieves the table size of a given tid
  """
  @spec get_table_size(table_key()) :: integer() | nil
  def get_table_size(sid_bid_pid) do
    with tid when tid != nil <- get_tid(sid_bid_pid),
         num when is_integer(num) <- :ets.info(tid, :size) do
      num
    else
      _ -> nil
    end
  end

  @doc """
  Returns the sum of all pending events across all queues of a source-backend combination.
  """
  @spec queues_pending_size(queues_key()) :: integer() | nil
  def queues_pending_size({sid, bid}) do
    list_pending_counts({sid, bid})
    |> Enum.reduce(0, fn {_sid_bid_tid, count}, acc ->
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
  @spec add_to_table(source_backend_pid() | queues_key(), [LogEvent.t()]) ::
          :ok | {:error, :not_initialized}
  def add_to_table({sid, bid} = sid_bid, batch) when is_integer(sid) do
    proc_counts =
      list_counts(sid_bid)
      |> Enum.sort_by(fn {_key, count} -> count end, :asc)
      |> Enum.filter(fn
        # exclude startup queue
        {{_, _, nil}, _} -> false
        {{_, _, _}, _} -> true
      end)

    procs = Enum.map(proc_counts, fn {key, _count} -> key end)

    if procs == [] do
      # not yet started, add to startup queue
      add_to_table({sid, bid, nil}, batch)
    else
      Logflare.Utils.chunked_round_robin(
        batch,
        procs,
        50,
        fn chunk, target ->
          add_to_table(target, chunk)
        end
      )
    end

    :ok
  end

  def add_to_table(sid_bid_pid, batch) do
    objects =
      for %{id: id} = event <- batch do
        {id, :pending, event}
      end

    get_tid(sid_bid_pid)
    |> case do
      nil ->
        {:error, :not_initialized}

      tid ->
        :ets.insert(tid, objects)
        :ok
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
  Returns a list of table keys for a source-backend combination.
  Startup queue is included.
  """
  @spec list_queues(queues_key()) :: [table_key()]
  def list_queues(sid_bid) do
    traverse_queues(
      sid_bid,
      fn objs, acc ->
        for {key, _tid} <- objs, reduce: acc do
          acc -> [key | acc]
        end
      end,
      []
    )
  end

  @doc """
  Deletes the queue associated with the given source-backend-pid.
  """
  @spec delete_queue(source_backend_pid()) :: :ok | {:error, :not_initialized}
  def delete_queue(sid_bid_pid) do
    with tid when tid != nil <- get_tid(sid_bid_pid) do
      :ets.delete(tid)
      :ets.delete(@ets_table_mapper, sid_bid_pid)
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
  @spec list_pending_counts(queues_key()) :: [{table_key(), non_neg_integer()}]
  def list_pending_counts(sid_bid) do
    traverse_queues(
      sid_bid,
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
  @spec list_counts(queues_key()) :: [{table_key(), non_neg_integer()}]
  def list_counts(sid_bid) do
    traverse_queues(
      sid_bid,
      fn objs, acc ->
        items =
          for {sid_bid_pid, _tid} <- objs, size = get_table_size(sid_bid_pid), is_integer(size) do
            {sid_bid_pid, size}
          end

        items ++ acc
      end,
      []
    )
  end

  @doc """
  Counts pending items from a given table
  """
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
  @spec truncate_table(source_backend_pid(), :all | :pending | :ingested, integer()) :: :ok

  def truncate_table({sid, _bid, _pid} = sid_bid_pid, :all, 0) when is_integer(sid) do
    # drop all objects
    with tid when tid != nil <- get_tid(sid_bid_pid) do
      :ets.delete_all_objects(tid)
      :ok
    else
      nil -> {:error, :not_initialized}
    end
  end

  def truncate_table({sid, _bid, _pid} = sid_bid_pid, status, n)
      when is_integer(sid) and status in [:all, :pending, :ingested] do
    # drop all objects

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

    with tid when tid != nil <- get_tid(sid_bid_pid),
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
  @spec delete_batch(source_backend_pid() | queues_key(), [LogEvent.t()]) :: :ok
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

  @spec drop(source_backend_pid(), :all | :pending | :ingested, non_neg_integer()) ::
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
    for {key, tid} <- to_check do
      if :ets.info(tid) == :undefined do
        :ets.delete(@ets_table_mapper, key)
      end
    end

    :ets.select(cont)
    |> next_and_cleanup()
  end

  @doc """
  Performs a reduce across all queues of a source-backend combination.

  Startup queue is included.
  """
  def traverse_queues({sid, bid}, func, acc \\ nil) do
    :ets.safe_fixtable(@ets_table_mapper, true)

    mapper_ms =
      Ex2ms.fun do
        {{^sid, ^bid, _pid}, tid} = obj -> obj
      end

    res =
      :ets.select(@ets_table_mapper, mapper_ms, 100)
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
