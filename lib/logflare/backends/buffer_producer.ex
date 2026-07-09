defmodule Logflare.Backends.BufferProducer do
  @moduledoc """
  A GenStage producer that pulls events from its own IngestEventQueue.
  In event that there are no events for the producer, it will periodically pull events from the queue.

  Supports both standard `{source_id, backend_id}` queues and consolidated `{:consolidated, backend_id}` queues.
  Pass `consolidated: true` in opts to use consolidated mode.
  """

  use GenStage

  require Logger

  alias Logflare.Backends.IngestEventQueue
  alias Logflare.LogEvent
  alias Logflare.Sources

  @type standard_state :: %{
          consolidated: boolean(),
          id_passing: boolean(),
          id_passing_metadata: boolean(),
          demand: non_neg_integer(),
          source_id: pos_integer() | nil,
          source_token: atom() | nil,
          backend_id: pos_integer(),
          last_discard_log_dt: DateTime.t() | nil,
          interval: pos_integer()
        }

  @type spool_producer_state :: %{
          spool_producer: true,
          consolidated: false,
          id_passing: boolean(),
          demand: non_neg_integer(),
          source_id: nil,
          source_token: nil,
          backend_id: nil,
          last_discard_log_dt: DateTime.t() | nil,
          interval: pos_integer()
        }

  @type state :: standard_state() | spool_producer_state()

  @type table_key :: {pos_integer() | atom(), pos_integer() | nil, pid() | nil}

  @default_interval 1_000
  @max_avg_before_pop 100

  def start_link(opts) when is_list(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  @impl GenStage
  def init(opts) do
    Process.flag(:trap_exit, true)

    spool_producer? = Keyword.get(opts, :spool_producer, false)
    consolidated? = Keyword.get(opts, :consolidated, false)
    backend_id = opts[:backend_id]
    interval = Keyword.get(opts, :interval, @default_interval)

    state =
      cond do
        spool_producer? -> init_spool_producer_state(interval)
        consolidated? -> init_consolidated_state(backend_id, interval, opts)
        true -> init_standard_state(opts, interval)
      end

    {:producer, state, buffer_size: Keyword.get(opts, :buffer_size, 10_000)}
  end

  @spec init_spool_producer_state(pos_integer()) :: state()
  defp init_spool_producer_state(interval) do
    state = %{
      spool_producer: true,
      consolidated: false,
      id_passing: true,
      demand: 0,
      source_id: nil,
      source_token: nil,
      backend_id: nil,
      last_discard_log_dt: nil,
      interval: interval
    }

    table_key = {:spool_producer, nil, self()}
    startup_table_key = {:spool_producer, nil, nil}
    IngestEventQueue.upsert_tid(table_key)
    IngestEventQueue.move(startup_table_key, table_key)
    schedule(state, false)

    state
  end

  @spec init_standard_state(keyword(), pos_integer()) :: state()
  defp init_standard_state(opts, interval) do
    source = Sources.Cache.get_by_id(opts[:source_id])

    state = %{
      consolidated: false,
      id_passing: Keyword.get(opts, :id_passing, false),
      id_passing_metadata: Keyword.get(opts, :id_passing_metadata, false),
      demand: 0,
      source_id: opts[:source_id],
      source_token: source.token,
      backend_id: opts[:backend_id],
      last_discard_log_dt: nil,
      interval: interval
    }

    table_key = {state.source_id, state.backend_id, self()}
    startup_table_key = {state.source_id, state.backend_id, nil}
    IngestEventQueue.upsert_tid(table_key)
    IngestEventQueue.move(startup_table_key, table_key)
    schedule(state, false)

    state
  end

  @spec init_consolidated_state(pos_integer(), pos_integer(), keyword()) :: state()
  defp init_consolidated_state(backend_id, interval, opts) do
    id_passing = Keyword.get(opts, :id_passing, false)
    id_passing_metadata = Keyword.get(opts, :id_passing_metadata, false)

    state = %{
      consolidated: true,
      demand: 0,
      id_passing: id_passing,
      id_passing_metadata: id_passing_metadata,
      source_id: nil,
      source_token: nil,
      backend_id: backend_id,
      last_discard_log_dt: nil,
      interval: interval
    }

    table_key = {:consolidated, backend_id, self()}
    startup_table_key = {:consolidated, backend_id, nil}
    IngestEventQueue.upsert_tid(table_key)
    IngestEventQueue.move(startup_table_key, table_key)

    if id_passing do
      IngestEventQueue.reset_processing_to_pending(table_key)
    end

    schedule(state, Keyword.get(opts, :scale, false))

    state
  end

  @impl GenStage
  def format_discarded(discarded, %{spool_producer: true} = state) do
    maybe_log_discarded(state, fn ->
      Logger.warning("Spool producer GenStage has discarded #{discarded} events from buffer")
    end)
  end

  def format_discarded(discarded, %{consolidated: true} = state) do
    maybe_log_discarded(state, fn ->
      Logger.warning(
        "Consolidated GenStage producer has discarded #{discarded} events from buffer",
        backend_id: state.backend_id
      )
    end)
  end

  def format_discarded(discarded, state) do
    source = Sources.Cache.get_by_id(state.source_token)

    maybe_log_discarded(state, fn ->
      Logger.warning(
        "GenStage producer for #{source.name} (#{source.token}) has discarded #{discarded} events from buffer",
        source_token: source.token,
        source_id: source.token,
        backend_id: state.backend_id
      )
    end)
  end

  @spec maybe_log_discarded(state :: state(), log_fn :: (-> any())) :: false
  defp maybe_log_discarded(state, log_fn) do
    should_log? =
      cond do
        state.last_discard_log_dt == nil -> true
        DateTime.diff(DateTime.utc_now(), state.last_discard_log_dt) > 5 -> true
        true -> false
      end

    if should_log? do
      log_fn.()
      send(self(), {:update_state, %{state | last_discard_log_dt: DateTime.utc_now()}})
    end

    false
  end

  @impl GenStage
  def handle_info(:scheduled_resolve, state) do
    {items, state} = resolve_demand(state)
    scale? = if Application.get_env(:logflare, :env) == :test, do: false, else: true
    schedule(state, scale?)
    {:noreply, items, state}
  end

  @impl GenStage
  def handle_info({:update_state, new_state}, _state) do
    {:noreply, [], new_state}
  end

  @impl GenStage
  def handle_info({:add_to_buffer, items}, state) do
    {:noreply, items, state}
  end

  @impl GenStage
  def handle_info({:EXIT, _caller_pid, _reason}, %{spool_producer: true} = state) do
    table_key = {:spool_producer, nil, self()}
    startup_table_key = {:spool_producer, nil, nil}
    IngestEventQueue.move(table_key, startup_table_key)
    {:noreply, [], state}
  end

  def handle_info({:EXIT, _caller_pid, _reason}, %{consolidated: true} = state) do
    table_key = {:consolidated, state.backend_id, self()}
    startup_table_key = {:consolidated, state.backend_id, nil}
    IngestEventQueue.move(table_key, startup_table_key)

    {:noreply, [], state}
  end

  def handle_info({:EXIT, _caller_pid, _reason}, state) do
    table_key = {state.source_id, state.backend_id, self()}
    startup_table_key = {state.source_id, state.backend_id, nil}
    IngestEventQueue.move(table_key, startup_table_key)

    {:noreply, [], state}
  end

  @impl GenStage
  def terminate(_reason, %{spool_producer: true} = state) do
    table_key = {:spool_producer, nil, self()}
    startup_table_key = {:spool_producer, nil, nil}
    IngestEventQueue.move(table_key, startup_table_key)
    state
  end

  def terminate(_reason, state), do: state

  @impl GenStage
  def handle_demand(demand, state) do
    {items, state} = resolve_demand(state, demand)
    {:noreply, items, state}
  end

  @spec schedule(state :: state(), scale? :: boolean()) :: reference()
  defp schedule(%{spool_producer: true} = state, _scale?) do
    Process.send_after(self(), :scheduled_resolve, state.interval)
  end

  defp schedule(%{consolidated: true} = state, _scale?) do
    Process.send_after(self(), :scheduled_resolve, state.interval)
  end

  defp schedule(state, scale?) do
    metrics = Sources.get_source_metrics_for_ingest(state.source_token)

    interval =
      cond do
        scale? == false ->
          state.interval

        metrics.avg < 10 ->
          state.interval * 5

        metrics.avg < 50 ->
          state.interval * 4

        metrics.avg < 100 ->
          state.interval * 3

        metrics.avg < 150 ->
          state.interval * 2

        metrics.avg < 250 ->
          state.interval * 1.5

        true ->
          state.interval
      end
      |> round()

    Process.send_after(self(), :scheduled_resolve, interval)
  end

  @spec resolve_demand(state :: state(), new_demand :: non_neg_integer()) ::
          {[LogEvent.t()], state()}
  defp resolve_demand(%{demand: prev_demand} = state, new_demand \\ 0) do
    total_demand = prev_demand + new_demand

    events = do_fetch(state, total_demand)
    event_count = Enum.count(events)

    new_demand =
      if total_demand < event_count do
        0
      else
        total_demand - event_count
      end

    {events, %{state | demand: new_demand}}
  end

  @spec do_fetch(state :: state(), count :: non_neg_integer()) :: [
          LogEvent.t()
          | {term(), :ets.tid(), non_neg_integer()}
          | {term(), :ets.tid(), non_neg_integer(), LogEvent.TypeDetection.event_type(),
             integer(), :fresh | :stale}
        ]
  defp do_fetch(
         %{consolidated: true, id_passing: true, id_passing_metadata: true, backend_id: bid},
         n
       ) do
    key = {:consolidated, bid, self()}

    case IngestEventQueue.take_pending_ids_with_metadata(key, n) do
      {:error, :not_initialized} ->
        []

      {:ok, [], _tid} ->
        []

      {:ok, metadata, tid} ->
        Enum.map(metadata, fn {id, size, event_type, day_bucket, freshness} ->
          {id, tid, size, event_type, day_bucket, freshness}
        end)
    end
  end

  defp do_fetch(%{consolidated: true, id_passing: true, backend_id: bid} = _state, n) do
    key = {:consolidated, bid, self()}

    case IngestEventQueue.take_pending_ids(key, n) do
      {:error, :not_initialized} -> []
      {:ok, [], _tid} -> []
      {:ok, id_size_pairs, tid} -> Enum.map(id_size_pairs, fn {id, size} -> {id, tid, size} end)
    end
  end

  defp do_fetch(%{consolidated: true, backend_id: bid} = _state, n) do
    key = {:consolidated, bid, self()}

    do_pop_key(key, n)
  end

  defp do_fetch(%{spool_producer: true} = _state, n) do
    key = {:spool_producer, nil, self()}

    case IngestEventQueue.take_pending_ids(key, n) do
      {:error, :not_initialized} ->
        Logger.warning("IngestEventQueue not initialized for spool_producer")
        []

      {:ok, [], _tid} ->
        []

      {:ok, id_size_pairs, tid} ->
        Enum.map(id_size_pairs, fn {id, size} -> {id, tid, size} end)
    end
  end

  defp do_fetch(
         %{source_id: sid, backend_id: bid, source_token: source_token, id_passing: id_passing} =
           _state,
         n
       ) do
    key = {sid, bid, self()}

    Sources.get_source_metrics_for_ingest(source_token)
    |> case do
      %{avg: avg} when avg > @max_avg_before_pop and not id_passing -> do_pop_key(key, n)
      _ -> do_take_key(key, n, id_passing)
    end
  end

  @spec do_take_key(key :: table_key(), count :: non_neg_integer(), id_passing :: boolean()) ::
          [LogEvent.t()] | [{term(), :ets.tid(), non_neg_integer()}]
  defp do_take_key({sid, bid, _pid} = key, n, true) do
    case IngestEventQueue.take_pending_ids(key, n) do
      {:error, :not_initialized} ->
        Logger.warning(
          "IngestEventQueue not initialized, could not fetch events. source_id: #{sid}",
          backend_id: bid
        )

        []

      {:ok, [], _tid} ->
        []

      {:ok, id_size_pairs, tid} ->
        Enum.map(id_size_pairs, fn {id, size} -> {id, tid, size} end)
    end
  end

  defp do_take_key({sid, bid, _pid} = key, n, false) do
    case IngestEventQueue.take_pending(key, n) do
      {:error, :not_initialized} ->
        Logger.warning(
          "IngestEventQueue not initialized, could not fetch events. source_id: #{sid}",
          backend_id: bid
        )

        []

      {:ok, []} ->
        []

      {:ok, events} ->
        {:ok, _} = IngestEventQueue.mark_ingested(key, events)

        events
    end
  end

  @spec do_pop_key(key :: table_key(), count :: non_neg_integer()) :: [
          LogEvent.t()
        ]
  defp do_pop_key({sid, bid, _pid} = key, n) do
    case IngestEventQueue.pop_pending(key, n) do
      {:error, :not_initialized} ->
        Logger.warning(
          "IngestEventQueue not initialized, could not fetch events. source_id: #{sid}",
          backend_id: bid
        )

        []

      {:ok, events} ->
        events
        |> Enum.map(fn %LogEvent{} = e -> %{e | is_popped: true} end)
    end
  end
end
