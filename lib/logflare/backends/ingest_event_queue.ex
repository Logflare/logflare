defmodule Logflare.Backends.IngestEventQueue do
  @moduledoc """
  GenServer will manage the ETS buffer mapping and own that table.

  :ets-backed buffer uses an :ets mapping pattern to fan out multiple :ets tables.
  """
  use GenServer
  alias Logflare.Source
  alias Logflare.Backends.Backend
  alias Logflare.LogEvent
  require Ex2ms

  @ets_table_mapper :ingest_event_queue_mapping
  @ets_table :source_ingest_events
  @typep source_backend_pid ::
           {Source.t() | non_neg_integer(), Backend.t() | nil | non_neg_integer(), pid()}
  @typep table_key :: {non_neg_integer(), nil | non_neg_integer(), pid()}
  @typep queues_key :: {non_neg_integer(), nil | non_neg_integer()}

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
  @spec get_tid(table_key()) :: reference() | nil
  def get_tid({sid, bid, pid}) do
    :ets.lookup_element(@ets_table_mapper, {sid, bid, pid}, 2, nil)
    # staleness check
    |> then(fn
      nil ->
        nil

      tid ->
        if(:ets.info(tid) != :undefined, do: tid)
    end)
  end

  @doc """
  Creates or updates a private :ets table. The :ets table mapper is stored in #{@ets_table_mapper} .
  """
  @spec upsert_tid(table_key()) ::
          {:ok, reference()} | {:error, :already_exists, reference()}
  def upsert_tid(sid_bid_pid) do
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
  @spec add_to_table(source_backend_pid(), [LogEvent.t()]) :: :ok | {:error, :not_initialized}
  def add_to_table({sid, bid} = sid_bid, batch) when is_integer(sid) do
    procs =
      list_counts(sid_bid)
      |> Enum.sort_by(fn {_key, count} -> count end, :asc)
      |> Enum.filter(fn
        # exclude startup queue
        {{_, _, nil}, _} -> false
        {{_, _, _}, _} -> true
      end)
      |> Enum.map(fn {key, _count} -> key end)

    procs_length = Enum.count(procs)

    if procs_length == 0 do
      # not yet started, add to startup queue
      add_to_table({sid, bid, nil}, batch)
    else
      Logflare.Utils.chunked_round_robin(
        batch,
        procs,
        100,
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
  @spec mark_ingested(source_backend_pid(), [LogEvent.t()]) :: :ok | {:error, :not_initialized}
  def mark_ingested(sid_bid_pid, events) do
    with tid when tid != nil <- get_tid(sid_bid_pid) do
      for event <- events do
        :ets.update_element(tid, event.id, {2, :ingested})
      end

      {:ok, Enum.count(events)}
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
              count = count_pending(sid_bid_pid),
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
  @spec count_pending(source_backend_pid()) :: integer() | {:error, :not_initialized}
  def count_pending({_, _} = sid_bid) do
    # iterate over each matching source-backend queue and sum the totals
    for {_sid_bid_pid, count} <- list_pending_counts(sid_bid), reduce: 0 do
      acc -> acc + count
    end
  end

  def count_pending({_sid, _bid, _pid} = sid_bid_pid) do
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
  Truncates queues
  """
  @spec truncate_table(source_backend_pid(), :all | :pending | :ingested, integer()) :: :ok

  def truncate_queues(sid_bid, filter, n) do
    # drop all objects
    traverse_queues(sid_bid, fn queue_objs, _acc ->
      for {sid_bid_pid, _tid} <- queue_objs do
        if get_table_size(sid_bid_pid) >= n do
          truncate_table(sid_bid_pid, filter, n)
        else
          truncate_table(sid_bid_pid, filter, 0)
        end
      end
    end)
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

  def truncate_table({sid, _bid, _pid} = sid_bid_pid, :ingested, 0) when is_integer(sid) do
    # drop all objects
    with tid when tid != nil <- get_tid(sid_bid_pid) do
      :ets.match_delete(tid, {:_, :ingested, :_})
      :ok
    else
      nil -> {:error, :not_initialized}
    end
  end

  def truncate_table({sid, _bid, _pid} = sid_bid_pid, filter, n)
      when is_integer(sid) and is_integer(n) and filter in [:pending, :all, :ingested] do
    # chunk over table and drop
    ms =
      Ex2ms.fun do
        {event_id, _status, _event} when ^filter == :all -> event_id
        {event_id, status, _event} when status == ^filter -> event_id
      end

    with tid when tid != nil <- get_tid(sid_bid_pid) do
      :ets.safe_fixtable(tid, true)
      res = truncate_traverse(tid, :ets.select(tid, ms, 100), 0, n)
      :ets.safe_fixtable(tid, false)
      res
    else
      nil -> {:error, :not_initialized}
      :"$end_of_table" -> {:ok, []}
    end
  end

  defp truncate_traverse(_tid, :"$end_of_table", _acc, _limit), do: :ok

  defp truncate_traverse(tid, {taken, cont}, acc, limit) when acc < limit do
    # keep
    diff = limit - acc
    rem = Enum.count(taken) - diff

    to_add =
      if rem > 0 do
        # satisfy all, drop up to required
        for key <- Enum.take(taken, diff) do
          :ets.delete(tid, key)
        end

        diff
      else
        # cannot satisfy all, keep all and continue
        Enum.count(taken)
      end

    truncate_traverse(tid, :ets.select(cont), acc + to_add, limit)
  end

  defp truncate_traverse(tid, {taken, cont}, acc, limit) when acc >= limit do
    # delete the rest
    for key <- taken, do: :ets.delete(tid, key)

    truncate_traverse(tid, :ets.select(cont), acc, limit)
  end

  @doc """
  Drop events from the ingest event table.
  """
  @spec drop(source_backend_pid(), :all | :pending | :ingested, non_neg_integer()) :: :ok

  def drop({_, _} = sid_bid, filter, n)
      when is_integer(n) and filter in [:pending, :all, :ingested] do
    traverse_queues(sid_bid, fn {sid_bid_pid, _tid}, acc ->
      {:ok, num} = drop(sid_bid_pid, filter, n)
      num + acc
    end)

    :ok
  end

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

  defp traverse_queues({sid, bid}, func, acc \\ nil) do
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
