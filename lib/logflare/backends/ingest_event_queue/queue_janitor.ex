defmodule Logflare.Backends.IngestEventQueue.QueueJanitor do
  @moduledoc """
  Performs cleanup actions for a private :ets queue

  Periodically purges the queue of `:ingested` events.

  If total events exceeds a max threshold, it will purge all events from the queue.
  This is in the case of sudden bursts of events that do not get cleared fast enough.
  It also acts as a failsafe for any potential runaway queue buildup from bugs.

  For consolidated queues, larger thresholds are used since they aggregate events
  from multiple sources.
  """
  use GenServer

  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Sources

  require Logger

  @default_interval 1_000
  @default_remainder 100
  @default_purge_ratio 0.05
  @default_max round(Logflare.Backends.max_buffer_queue_len() * 1.2)
  @consolidated_max_multiplier 10
  @stale_processing_interval 10_000
  @max_stale_retries 3

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def init(opts) do
    bid = if backend = Keyword.get(opts, :backend), do: backend.id
    source = Keyword.get(opts, :source)
    consolidated? = Keyword.get(opts, :consolidated, false)
    consolidated_key = Keyword.get(opts, :consolidated_key)
    base_max = Keyword.get(opts, :max, @default_max)

    state = %{
      source_id: source.id,
      source_token: source.token,
      backend_id: bid,
      interval: Keyword.get(opts, :interval, @default_interval),
      remainder: Keyword.get(opts, :remainder, @default_remainder),
      max: if(consolidated?, do: base_max * @consolidated_max_multiplier, else: base_max),
      purge_ratio: Keyword.get(opts, :purge_ratio, @default_purge_ratio),
      consolidated?: consolidated?,
      consolidated_key: consolidated_key,
      processing_snapshot: %{}
    }

    handle_info(:work, state)
    Process.send_after(self(), :cleanup_stale_processing, @stale_processing_interval)
    {:ok, state}
  end

  def handle_info(:cleanup_stale_processing, state) do
    state = do_cleanup_stale_processing(state)
    Process.send_after(self(), :cleanup_stale_processing, @stale_processing_interval)
    {:noreply, state}
  end

  def handle_info(:work, state) do
    scale? = if Application.get_env(:logflare, :env) == :test, do: false, else: true
    metrics = Sources.get_source_metrics_for_ingest(state.source_token)
    do_drop(state, metrics)

    schedule(state, scale?, metrics)

    {:noreply, state}
  end

  # expose for benchmarking
  def do_drop(%{consolidated?: true, consolidated_key: consolidated_key} = state, metrics) do
    for {:consolidated, _bid, pid} = table_key <- IngestEventQueue.list_queues(consolidated_key) do
      truncate_all? = metrics.avg > 100
      drop_queue(state, table_key, pid, truncate_all?, :consolidated)
    end
  end

  def do_drop(state, metrics) do
    sid_bid = {state.source_id, state.backend_id}

    for {_sid, bid, pid} = table_key <- IngestEventQueue.list_queues(sid_bid) do
      truncate_all? = metrics.avg > 100 or bid != nil
      drop_queue(state, table_key, pid, truncate_all?, :source)
    end
  end

  @spec drop_queue(
          map(),
          {pos_integer(), pos_integer() | nil, pid() | nil}
          | {:consolidated, pos_integer(), pid() | nil},
          pid() | nil,
          boolean(),
          :consolidated | :source
        ) :: :ok | nil
  defp drop_queue(state, table_key, pid, truncate_all?, queue_type) do
    if truncate_all? do
      IngestEventQueue.truncate_table(table_key, :ingested, 0)
    else
      IngestEventQueue.truncate_table(table_key, :ingested, state.remainder)
    end

    size = IngestEventQueue.get_table_size(table_key)

    if is_integer(size) and size > state.max and pid != nil do
      to_drop = round(state.purge_ratio * size)
      IngestEventQueue.drop(table_key, :pending, to_drop)

      log_msg =
        case queue_type do
          :consolidated ->
            "IngestEventQueue consolidated :ets buffer exceeded max for backend_id=#{state.backend_id}, dropping #{to_drop} events"

          :source ->
            "IngestEventQueue private :ets buffer exceeded max for source id=#{state.source_id}, dropping #{to_drop} events"
        end

      Logger.warning(log_msg,
        backend_id: state.backend_id,
        source_id: state.source_token,
        source_token: state.source_token,
        ingest_drop_count: to_drop
      )
    end
  end

  # Snapshot-based stale :processing cleanup.
  # On each run, get current :processing IDs per queue. Any IDs that were also
  # :processing on the previous run are stuck (batcher crash, kill signal, etc.).
  # Stuck events are reset to :pending and retried.
  # Dropped once they've been stale @max_stale_retries times.
  # expose for testing
  def do_cleanup_stale_processing(
        %{consolidated?: true, consolidated_key: consolidated_key} = state
      ) do
    queues = IngestEventQueue.list_queues(consolidated_key)
    do_cleanup_queues(state, queues)
  end

  def do_cleanup_stale_processing(state) do
    sid_bid = {state.source_id, state.backend_id}
    queues = IngestEventQueue.list_queues(sid_bid)
    do_cleanup_queues(state, queues)
  end

  defp do_cleanup_queues(state, queues) do
    new_snapshot =
      Enum.reduce(queues, %{}, fn table_key, snapshot ->
        current_ids = MapSet.new(IngestEventQueue.list_processing_ids(table_key))
        prev_ids = Map.get(state.processing_snapshot, table_key, MapSet.new())
        stale_ids = MapSet.intersection(current_ids, prev_ids) |> MapSet.to_list()

        {n_reset, n_drop} = process_stale_events(table_key, stale_ids)
        if n_reset + n_drop > 0, do: emit_stale_telemetry(state, n_reset, n_drop)

        Map.put(snapshot, table_key, current_ids)
      end)

    %{state | processing_snapshot: new_snapshot}
  end

  defp process_stale_events(_table_key, []), do: {0, 0}

  defp process_stale_events(table_key, stale_ids) do
    with tid when tid != nil <- IngestEventQueue.get_tid(table_key) do
      Enum.reduce(stale_ids, {0, 0}, fn id, {resets, drops} ->
        case act_on_stale_event(tid, id) do
          :reset -> {resets + 1, drops}
          :drop -> {resets, drops + 1}
          :skip -> {resets, drops}
        end
      end)
    else
      _ -> {0, 0}
    end
  end

  defp act_on_stale_event(tid, id) do
    case :ets.lookup(tid, id) do
      [{^id, :processing, %{retries: retries}, _size}] when retries >= @max_stale_retries - 1 ->
        :ets.select_delete(tid, [{{id, :processing, :_, :_}, [], [true]}])
        :drop

      [{^id, :processing, %{retries: retries} = le, size}] ->
        new_le = %{le | retries: (retries || 0) + 1}

        case :ets.select_replace(tid, [
               {{id, :processing, le, size}, [], [{:const, {id, :pending, new_le, size}}]}
             ]) do
          1 -> :reset
          0 -> :skip
        end

      _ ->
        :skip
    end
  end

  defp emit_stale_telemetry(state, n_reset, n_drop) do
    :telemetry.execute(
      [:logflare, :ingest_event_queue, :stale_processing],
      %{reset: n_reset, dropped: n_drop},
      %{source_id: state.source_id}
    )

    Logger.warning(
      "QueueJanitor: reset #{n_reset} and dropped #{n_drop} stale :processing events",
      source_id: state.source_id,
      backend_id: state.backend_id
    )
  end

  # schedule work based on rps
  defp schedule(state, scale?, metrics) do
    # dynamically schedule based on metrics interval
    interval =
      cond do
        scale? == false ->
          state.interval

        metrics.avg < 100 ->
          state.interval * 10

        metrics.avg < 1000 ->
          state.interval * 5

        metrics.avg < 2000 ->
          state.interval * 2.5

        true ->
          state.interval
      end
      |> round()

    Process.send_after(self(), :work, interval)
  end
end
