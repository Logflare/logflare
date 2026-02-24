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

  @type state :: %{
          consolidated: boolean(),
          demand: non_neg_integer(),
          source_id: pos_integer() | nil,
          source_token: atom() | nil,
          backend_id: pos_integer(),
          last_discard_log_dt: DateTime.t() | nil,
          interval: pos_integer()
        }

  @default_interval 1_000

  def start_link(opts) when is_list(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  @impl GenStage
  def init(opts) do
    Process.flag(:trap_exit, true)

    consolidated? = Keyword.get(opts, :consolidated, false)
    backend_id = opts[:backend_id]
    interval = Keyword.get(opts, :interval, @default_interval)

    state =
      if consolidated? do
        init_consolidated_state(backend_id, interval, opts)
      else
        init_standard_state(opts, interval)
      end

    {:producer, state, buffer_size: Keyword.get(opts, :buffer_size, 10_000)}
  end

  @spec init_standard_state(keyword(), pos_integer()) :: state()
  defp init_standard_state(opts, interval) do
    source = Sources.Cache.get_by_id(opts[:source_id])

    state = %{
      consolidated: false,
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
    state = %{
      consolidated: true,
      demand: 0,
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
  def handle_demand(demand, state) do
    {items, state} = resolve_demand(state, demand)
    {:noreply, items, state}
  end

  @spec schedule(state :: state(), scale? :: boolean()) :: reference()
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

  @spec do_fetch(state :: state(), count :: non_neg_integer()) :: [LogEvent.t()]
  defp do_fetch(%{consolidated: true, backend_id: bid}, n) do
    key = {:consolidated, bid, self()}

    case IngestEventQueue.pop_pending(key, n) do
      {:error, :not_initialized} ->
        Logger.error(
          "IngestEventQueue not initialized for consolidated queue",
          backend_id: bid
        )

        []

      {:ok, events} ->
        events
    end
  end

  defp do_fetch(%{source_id: sid, backend_id: bid}, n) do
    key = {sid, bid, self()}

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
end
