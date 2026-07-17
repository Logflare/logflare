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
          interval: pos_integer(),
          in_flight_ref: :atomics.atomics_ref() | nil,
          max_in_flight: non_neg_integer() | :infinity | nil
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
          interval: pos_integer(),
          in_flight_ref: :atomics.atomics_ref() | nil,
          max_in_flight: non_neg_integer() | :infinity | nil
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
        spool_producer? -> init_spool_producer_state(interval, opts)
        consolidated? -> init_consolidated_state(backend_id, interval, opts)
        true -> init_standard_state(opts, interval)
      end

    {:producer, state, buffer_size: Keyword.get(opts, :buffer_size, 10_000)}
  end

  # Publishes a reference to a mutable :atomics counter (not the counter value itself)
  # via :persistent_term, so transform/2 — which runs in this same process, per
  # Broadway.Topology.ProducerStage — can look it up once and hand it to ack/3 through
  # the message's ack_data. Only :persistent_term.put/2 (here, once per producer
  # lifecycle) and :erase/1 (in terminate/2) are ever called; the actual claim/ack
  # traffic goes straight through the atomics ref, since repeated :persistent_term
  # writes would trigger a global VM-wide sweep on every single event.
  @spec init_in_flight(id_passing :: boolean(), max_in_flight :: non_neg_integer() | nil) ::
          {:atomics.atomics_ref() | nil, non_neg_integer() | :infinity | nil}
  defp init_in_flight(true, max_in_flight) do
    ref = :atomics.new(1, signed: true)
    :persistent_term.put({__MODULE__, :in_flight_ref, self()}, ref)
    {ref, max_in_flight || :infinity}
  end

  defp init_in_flight(false, _max_in_flight), do: {nil, nil}

  # One-time, capped pre-seed of a freshly (re)started producer's own queue from the
  # shared startup queue — bounded to max_in_flight so it never absorbs more backlog
  # than it could ever claim anyway, while giving it real pending work immediately
  # instead of waiting on however many ticks do_fetch's own startup fallback would take
  # to reach the same amount. Uses pop_pending_pointers' atomic :ets.take-gated claim
  # (safe even if another producer is concurrently draining the same startup table)
  # then relocates each pointer's row into the new own table by overriding queue_tid
  # before reinsert_pointer/1 — a one-off exception to that function's usual "reinsert
  # where it was claimed from" contract. Only meaningful for id-passing producers with a
  # finite cap; max_in_flight is nil for non-id-passing producers and :infinity for
  # id-passing ones with no configured cap.
  @spec seed_from_startup(table_key(), table_key(), non_neg_integer() | :infinity | nil) :: :ok
  defp seed_from_startup(_startup_key, _own_key, nil), do: :ok
  defp seed_from_startup(_startup_key, _own_key, :infinity), do: :ok

  defp seed_from_startup(startup_key, own_key, max_in_flight) when is_integer(max_in_flight) do
    case IngestEventQueue.pop_pending_pointers(startup_key, max_in_flight) do
      {:error, :not_initialized} ->
        :ok

      {:ok, [], _tid} ->
        :ok

      {:ok, pointers, _tid} ->
        own_tid = IngestEventQueue.get_tid(own_key)

        Enum.each(pointers, fn pointer ->
          IngestEventQueue.reinsert_pointer(%{pointer | queue_tid: own_tid})
        end)

        :ok
    end
  end

  @spec init_spool_producer_state(pos_integer(), keyword()) :: state()
  defp init_spool_producer_state(interval, opts) do
    {in_flight_ref, max_in_flight} = init_in_flight(true, Keyword.get(opts, :max_in_flight))

    state = %{
      spool_producer: true,
      consolidated: false,
      id_passing: true,
      demand: 0,
      source_id: nil,
      source_token: nil,
      backend_id: nil,
      last_discard_log_dt: nil,
      interval: interval,
      in_flight_ref: in_flight_ref,
      max_in_flight: max_in_flight
    }

    table_key = {:spool_producer, nil, self()}
    startup_table_key = {:spool_producer, nil, nil}
    IngestEventQueue.upsert_tid(table_key)
    seed_from_startup(startup_table_key, table_key, max_in_flight)
    schedule(state, false)

    state
  end

  @spec init_standard_state(keyword(), pos_integer()) :: state()
  defp init_standard_state(opts, interval) do
    source = Sources.Cache.get_by_id(opts[:source_id])
    id_passing = Keyword.get(opts, :id_passing, false)
    {in_flight_ref, max_in_flight} = init_in_flight(id_passing, Keyword.get(opts, :max_in_flight))

    state = %{
      consolidated: false,
      id_passing: id_passing,
      demand: 0,
      source_id: opts[:source_id],
      source_token: source.token,
      backend_id: opts[:backend_id],
      last_discard_log_dt: nil,
      interval: interval,
      in_flight_ref: in_flight_ref,
      max_in_flight: max_in_flight
    }

    table_key = {state.source_id, state.backend_id, self()}
    startup_table_key = {state.source_id, state.backend_id, nil}
    IngestEventQueue.upsert_tid(table_key)
    seed_from_startup(startup_table_key, table_key, max_in_flight)
    schedule(state, false)

    state
  end

  @spec init_consolidated_state(pos_integer(), pos_integer(), keyword()) :: state()
  defp init_consolidated_state(backend_id, interval, opts) do
    id_passing = Keyword.get(opts, :id_passing, false)
    {in_flight_ref, max_in_flight} = init_in_flight(id_passing, Keyword.get(opts, :max_in_flight))

    state = %{
      consolidated: true,
      demand: 0,
      id_passing: id_passing,
      source_id: nil,
      source_token: nil,
      backend_id: backend_id,
      last_discard_log_dt: nil,
      interval: interval,
      in_flight_ref: in_flight_ref,
      max_in_flight: max_in_flight
    }

    table_key = {:consolidated, backend_id, self()}
    startup_table_key = {:consolidated, backend_id, nil}
    IngestEventQueue.upsert_tid(table_key)
    seed_from_startup(startup_table_key, table_key, max_in_flight)

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
    cleanup_in_flight_ref(state)
    table_key = {:spool_producer, nil, self()}
    startup_table_key = {:spool_producer, nil, nil}
    IngestEventQueue.move(table_key, startup_table_key)
    state
  end

  def terminate(_reason, state) do
    cleanup_in_flight_ref(state)
    state
  end

  defp cleanup_in_flight_ref(%{in_flight_ref: ref}) when not is_nil(ref) do
    :persistent_term.erase({__MODULE__, :in_flight_ref, self()})
  end

  defp cleanup_in_flight_ref(_state), do: :ok

  # Diagnostic getter only — atomics refs can't be sent across a distributed
  # connection and read remotely (unlike the plain integer this returns), so callers
  # inspecting a producer's in-flight count from another node must do it in one
  # :rpc.call that runs entirely on the producer's own node.
  @spec in_flight_count(pid()) :: non_neg_integer() | nil
  def in_flight_count(pid) do
    case :persistent_term.get({__MODULE__, :in_flight_ref, pid}, nil) do
      nil -> nil
      ref -> :atomics.get(ref, 1)
    end
  end

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
    fetch_amount = capped_fetch_amount(state, total_demand)

    events = do_fetch(state, fetch_amount)
    event_count = Enum.count(events)

    if state.in_flight_ref != nil and event_count > 0 do
      :atomics.add(state.in_flight_ref, 1, event_count)
    end

    new_demand =
      if total_demand < event_count do
        0
      else
        total_demand - event_count
      end

    {events, %{state | demand: new_demand}}
  end

  # A generous safety valve, not a fine-grained flow-control knob: caps how much a
  # producer will claim from IngestEventQueue once too much already-claimed work is
  # sitting unacked (e.g. stuck deep in Broadway's own, effectively unbounded batcher
  # buffering — see BufferProducer moduledoc/callers for context). Non-id-passing
  # producers have no in_flight_ref and are never throttled.
  @spec capped_fetch_amount(state :: state(), requested :: non_neg_integer()) ::
          non_neg_integer()
  defp capped_fetch_amount(%{in_flight_ref: nil}, requested), do: requested
  defp capped_fetch_amount(%{max_in_flight: :infinity}, requested), do: requested

  defp capped_fetch_amount(%{in_flight_ref: ref, max_in_flight: max_in_flight}, requested) do
    current = :atomics.get(ref, 1)
    available = max(max_in_flight - current, 0)
    min(requested, available)
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
  # safe against the startup key's multiple concurrent readers (see fetch_own_first/4).
  # Doesn't record into the recent-events cache: none of the non-id-passing adaptors
  # (webhook, syslog, http_based, postgres, s3) need deferred "recent logs" visibility
  # the way BigQuery's ack does, and pop_pending has already deleted the
  # generation-store row as part of claiming anyway, so there'd be nothing left to defer
  # deleting even if they did.
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
