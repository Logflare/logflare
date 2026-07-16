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
  alias Logflare.Backends.IngestEventQueue.LogEventPointer
  alias Logflare.LogEvent
  alias Logflare.Sources

  @type standard_state :: %{
          consolidated: boolean(),
          id_passing: boolean(),
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
    id_passing = Keyword.get(opts, :id_passing, false)

    state = %{
      consolidated: false,
      id_passing: id_passing,
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

    state = %{
      consolidated: true,
      demand: 0,
      id_passing: id_passing,
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

  # Every producer services its own queue first — a producer that's over
  # max_queue_size (the reason anything lands in startup at all) needs to clear its
  # own dedicated backlog to become round-robin-eligible again, and splitting its
  # attention with the shared startup queue instead only delays that. The shared
  # startup queue is only pulled from to top off whatever demand the producer's own
  # queue couldn't satisfy — a brand-new producer's own queue is empty anyway, so it
  # falls through to startup immediately regardless; an already-loaded producer just
  # keeps grinding its own queue down instead of competing for the shared one.
  @spec do_fetch(state :: state(), count :: non_neg_integer()) :: [
          LogEvent.t() | LogEventPointer.t()
        ]
  defp do_fetch(%{consolidated: true, id_passing: true, backend_id: bid}, n) do
    own_key = {:consolidated, bid, self()}
    startup_key = {:consolidated, bid, nil}
    fetch_own_first(own_key, startup_key, n, &fetch_pointers/2)
  end

  defp do_fetch(%{consolidated: true, backend_id: bid} = _state, n) do
    own_key = {:consolidated, bid, self()}
    startup_key = {:consolidated, bid, nil}
    fetch_own_first(own_key, startup_key, n, &do_pop_key/2)
  end

  defp do_fetch(%{spool_producer: true} = _state, n) do
    own_key = {:spool_producer, nil, self()}
    startup_key = {:spool_producer, nil, nil}
    fetch_own_first(own_key, startup_key, n, &fetch_pointers/2)
  end

  defp do_fetch(
         %{source_id: sid, backend_id: bid, id_passing: id_passing} = _state,
         n
       ) do
    own_key = {sid, bid, self()}
    startup_key = {sid, bid, nil}

    if id_passing do
      fetch_own_first(own_key, startup_key, n, &fetch_pointers/2)
    else
      fetch_own_first(own_key, startup_key, n, &do_pop_key/2)
    end
  end

  # The startup key can be claimed from by more than one live producer at once, so
  # every fetch function passed here must resolve via an atomic claim primitive
  # (:ets.take-gated) — see do_pop_key/2 and fetch_pointers/2, the only two in use.
  @spec fetch_own_first(
          own_key :: table_key(),
          startup_key :: table_key(),
          n :: non_neg_integer(),
          fetch_fn :: (table_key(), non_neg_integer() -> [term()])
        ) :: [term()]
  defp fetch_own_first(own_key, startup_key, n, fetch_fn) do
    own_events = fetch_fn.(own_key, n)
    remaining = n - length(own_events)

    if remaining > 0 do
      own_events ++ fetch_fn.(startup_key, remaining)
    else
      own_events
    end
  end

  defp fetch_pointers(key, n) do
    case IngestEventQueue.pop_pending_pointers(key, n) do
      {:error, :not_initialized} -> []
      {:ok, pointers, _tid} -> pointers
    end
  end

  # Only fetch primitive for non-id-passing producers now — pop_pending atomically
  # resolves both the pointer row and the generation-store event in one step, so it's
  # safe against the startup key's multiple concurrent readers (see
  # fetch_own_first/4). Recording into the recent-events cache here (since
  # none of the non-id-passing adaptors — webhook, syslog, http_based, postgres, s3 —
  # implement real ack/retry logic today, this is the only place left that can) is
  # gated the same way Source.BigQuery.Pipeline's own should_record_recent?/1 gates its
  # ack-time recording: skip it once the ingest rate is high enough that the extra
  # lookup isn't worth it. Never recorded for consolidated queues, which have no single
  # source to resolve metrics for.
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
        Enum.map(events, fn %LogEvent{} = e ->
          %{e | is_popped: true}
        end)
    end
  end
end
