defmodule Logflare.Backends.IngestEvents do
  @moduledoc false
  use GenServer
  alias Logflare.Source
  require Ex2ms

  @ets_table_mapper :source_ingest_events_reference
  @ets_table :source_ingest_events

  ## Server
  def start_link(_args) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__, hibernate_after: 1_000)
  end

  def init(_args) do
    :ets.new(@ets_table_mapper, [:public, :named_table, :set])
    {:ok, %{}}
  end

  def get_tid(%Source{id: id}) do
    :ets.lookup_element(@ets_table_mapper, id, 2, nil)
  end

  def upsert_tid(%Source{id: id} = source) do
    case get_tid(source) do
    nil ->
      # create and insert
      tid = :ets.new(@ets_table, [:public, :duplicate_bag, :compressed, {:write_concurrency, true}, {:read_concurrency, true}])
      :ets.insert(@ets_table_mapper, {id, tid})
      {:ok, tid}
    tid -> {:error, :already_exists, tid}
    end
  end

  def get_table_size(%Source{id: id}= source) do
    get_tid(source)
    |> case do
      nil -> nil
      tid -> :ets.info(tid, :size)
    end
  end

  def get_table_info(%Source{id: id}= source) do
    get_tid(source)
    |> case do
      nil -> nil
      tid -> :ets.info(tid)
    end
  end
  def add_to_table(source, batch) do
    objects = for %{id: id} =  event <- batch do
      {id, event}
    end
    get_tid(source)
    |> case do
      nil -> {:error, :not_initialized}
      tid ->
        :ets.insert(tid, objects)
      :ok
    end
  end

  def dirty_pop(source, n) do

    ms =
      Ex2ms.fun do
        {_id, event}  -> event
      end
    with tid when tid != nil <- get_tid(source),
    {popped, _cont} <- :ets.select(tid, ms, n)  do
      {:ok, popped}

    else
      nil ->  {:error, :not_initialized}
      :"$end_of_table" -> {:ok, []}
    end

  end
end
