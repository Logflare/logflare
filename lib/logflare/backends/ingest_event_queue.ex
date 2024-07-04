defmodule Logflare.Backends.IngestEventQueue do
  @moduledoc false
  use GenServer
  alias Logflare.Source
  alias Logflare.Backends.Backend
  require Ex2ms

  @ets_table_mapper :ingest_event_queue_mapping
  @ets_table :source_ingest_events

  ## Server
  def start_link(_args) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__, hibernate_after: 1_000)
  end

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
  def get_tid({%Source{id: sid}, nil}), do: get_tid({sid, nil})
  def get_tid({%Source{id: sid}, %Backend{id: bid}}), do: get_tid({sid, bid})

  def get_tid({sid, bid}) do
    :ets.lookup_element(@ets_table_mapper, {sid, bid}, 2, nil)
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
  def upsert_tid({%Source{id: sid}, nil}), do: upsert_tid({sid, nil})
  def upsert_tid({%Source{id: sid}, %Backend{id: bid}}), do: upsert_tid({sid, bid})

  def upsert_tid({sid, bid} = sid_bid) when is_integer(sid) do
    case get_tid(sid_bid) do
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

        :ets.insert(@ets_table_mapper, {{sid, bid}, tid})
        {:ok, tid}

      tid ->
        {:error, :already_exists, tid}
    end
  end

  @doc """
  Retrieves the table size of a given tid
  """
  def get_table_size({%Source{id: sid}, nil}), do: get_table_size({sid, nil})
  def get_table_size({%Source{id: sid}, %Backend{id: bid}}), do: get_table_size({sid, bid})

  def get_table_size({sid, bid}) do
    with tid when tid != nil <- get_tid({sid, bid}),
         num when is_integer(num) <- :ets.info(tid, :size) do
      num
    else
      _ -> nil
    end
  end

  @doc """
  Retrieves the :ets.info/1 of a table
  """
  def get_table_info({_source, _backend} = sb) do
    get_tid(sb)
    |> case do
      nil -> nil
      tid -> :ets.info(tid)
    end
  end

  @doc """
  Adds a record to a given source-backend's table queue.

  The record will be marked as :pending.
  """
  def add_to_table({%Source{id: sid}, nil}, batch), do: add_to_table({sid, nil}, batch)

  def add_to_table({%Source{id: sid}, %Backend{id: bid}}, batch),
    do: add_to_table({sid, bid}, batch)

  def add_to_table({sid, _bid} = sid_bid, batch) when is_integer(sid) do
    objects =
      for %{id: id} = event <- batch do
        {id, :pending, event}
      end

    get_tid(sid_bid)
    |> case do
      nil ->
        {:error, :not_initialized}

      tid ->
        :ets.insert(tid, objects)
        :ok
    end
  end

  @doc """
  Marks records as ingested
  """
  def mark_ingested({%Source{id: sid}, nil}, events), do: mark_ingested({sid, nil}, events)

  def mark_ingested({%Source{id: sid}, %Backend{id: bid}}, events),
    do: mark_ingested({sid, bid}, events)

  def mark_ingested({sid, _bid} = sid_bid, events) when is_integer(sid) do
    with tid when tid != nil <- get_tid(sid_bid) do
      updated =
        for event <- events do
          {event.id, :ingested, event}
        end

      :ets.insert(tid, updated)

      {:ok, Enum.count(events)}
    else
      nil -> {:error, :not_initialized}
    end
  end

  @doc """
  Counts pending items from a given table
  """
  def count_pending({%Source{} = source, nil}), do: count_pending({source.id, nil})
  def count_pending({%Source{id: sid}, %Backend{id: bid}}), do: count_pending({sid, bid})

  def count_pending({sid, _bid} = sid_bid) when is_integer(sid) do
    ms =
      Ex2ms.fun do
        {_event_id, :pending, _event} -> true
        {_event_id, :pending, _event} -> true
      end

    with tid when tid != nil <- get_tid(sid_bid),
         num when is_integer(num) <- :ets.select_count(tid, ms) do
      num
    else
      nil -> {:error, :not_initialized}
    end
  end

  @doc """
  Takes pending items from a given table
  """
  def take_pending({%Source{} = source, nil}, n), do: take_pending({source.id, nil}, n)
  def take_pending({%Source{id: sid}, %Backend{id: bid}}, n), do: take_pending({sid, bid}, n)

  def take_pending({sid, _bid} = sid_bid, 0) when is_integer(sid), do: {:ok, []}

  def take_pending({sid, _bid} = sid_bid, n) when is_integer(sid) and is_integer(n) do
    ms =
      Ex2ms.fun do
        {_event_id, :pending, event} -> event
      end

    with tid when tid != nil <- get_tid(sid_bid),
         size when is_integer(size) <- :ets.info(tid, :size),
         {taken, _cont} <- :ets.select(tid, ms, min(n, max(size, 1))) do
      {:ok, taken}
    else
      nil -> {:error, :not_initialized}
      :"$end_of_table" -> {:ok, []}
    end
  end

  @doc """
  Truncates a given table
  """
  def truncate({%Source{} = source, nil}, filter, n), do: truncate({source.id, nil}, filter, n)

  def truncate({%Source{id: sid}, %Backend{id: bid}}, filter, n),
    do: truncate({sid, bid}, filter, n)

  def truncate({sid, _bid} = sid_bid, :all, 0) when is_integer(sid) do
    # drop all objects
    with tid when tid != nil <- get_tid(sid_bid) do
      :ets.delete_all_objects(tid)
      :ok
    else
      nil -> {:error, :not_initialized}
    end
  end

  def truncate({sid, _bid} = sid_bid, filter, n)
      when is_integer(sid) and is_integer(n) and filter in [:pending, :all, :ingested] do
    # chunk over table and drop
    ms =
      Ex2ms.fun do
        {_event_id, _status, _event} = obj when ^filter == :all -> obj
        {_event_id, status, _event} = obj when status == ^filter -> obj
      end

    with tid when tid != nil <- get_tid(sid_bid) do
      :ets.safe_fixtable(tid, true)
      res = truncate_traverse(tid, :ets.select(tid, ms, 1000), 0, n)
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
        for {key, _status, _event} <- Enum.take(taken, diff) do
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
    for {key, _status, _event} <- taken do
      :ets.delete(tid, key)
    end

    truncate_traverse(tid, :ets.select(cont), acc, limit)
  end

  def delete_all_mappings do
    :ets.delete_all_objects(@ets_table_mapper)
  end

  def delete_stale_mappings do
    ms =
      Ex2ms.fun do
        obj -> obj
      end

    :ets.select(@ets_table_mapper, ms, 100)
    |> next_and_cleanup()
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
end
