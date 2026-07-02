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

  # Stale :processing cleanup is crash recovery, not normal queue maintenance: in the healthy
  # path a row is only :processing between claim and ack (bounded to ~10s by the batch timeout
  # and the BigQuery insert HTTP timeouts). A row still :processing after the staleness age has
  # been orphaned by a crashed/hung pipeline, so it is recovered. Sustained resets/drops point
  # to a correctness issue (lost acks, pipeline crashes, backend latency) and should be
  # investigated, not treated as throughput.
  #
  # The age sits well above the ~10s live ceiling to avoid resetting a slow-but-live batch, and
  # the per-pass limit is sized to clear a crashed pipeline's in-flight cohort (processors +
  # batchers) in a single pass so orphaned rows do not squat on the queue's size budget.
  @stale_processing_interval :timer.seconds(60)
  @stale_processing_age_ms :timer.seconds(30)
  @stale_processing_limit 10_000
  @max_stale_retries 3

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc """
  Age in milliseconds after which a `:processing` row is considered stale. Exposed for tests.
  """
  @spec stale_processing_age_ms() :: pos_integer()
  def stale_processing_age_ms, do: @stale_processing_age_ms

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
      stale_processing_limit: Keyword.get(opts, :stale_processing_limit, @stale_processing_limit)
    }

    handle_info(:work, state)

    unless consolidated?,
      do: Process.send_after(self(), :cleanup_stale_processing, @stale_processing_interval)

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

  # Timestamp-based stale :processing cleanup.
  # Each :processing row carries the monotonic claim time (claimed_at). A row claimed longer
  # ago than @stale_processing_age_ms is stuck (batcher crash, kill signal, lost ack, etc.),
  # so it is reset to :pending and retried, or dropped once it has been stale
  # @max_stale_retries times. Each pass acts on at most @stale_processing_limit stale rows per
  # queue; ETS may still scan beyond that limit to find matching stale rows. Any overflow is
  # recovered on later passes.
  # expose for testing
  def do_cleanup_stale_processing(state, now \\ System.monotonic_time(:millisecond))

  def do_cleanup_stale_processing(%{consolidated?: true} = state, _now) do
    state
  end

  def do_cleanup_stale_processing(state, now) do
    cutoff = now - @stale_processing_age_ms
    sid_bid = {state.source_id, state.backend_id}

    for table_key <- IngestEventQueue.list_queues(sid_bid) do
      {n_reset, n_drop} = cleanup_queue(state, table_key, cutoff)
      if n_reset + n_drop > 0, do: emit_stale_telemetry(state, n_reset, n_drop)
    end

    state
  end

  defp cleanup_queue(state, table_key, cutoff) do
    limit = state.stale_processing_limit
    stale_ids = IngestEventQueue.list_stale_processing_ids(table_key, cutoff, limit)

    if length(stale_ids) >= limit do
      Logger.warning(
        "QueueJanitor: stale :processing cleanup hit per-pass limit of #{limit}; remaining stale rows will be handled next pass",
        source_id: state.source_id,
        backend_id: state.backend_id
      )
    end

    process_stale_events(table_key, stale_ids)
  end

  defp process_stale_events(_table_key, []), do: {0, 0}

  defp process_stale_events(table_key, stale_ids) do
    case IngestEventQueue.get_tid(table_key) do
      nil -> {0, 0}
      tid -> tally_stale_events(tid, stale_ids)
    end
  end

  defp tally_stale_events(tid, stale_ids) do
    Enum.reduce(stale_ids, {0, 0}, fn id, {resets, drops} ->
      case act_on_stale_event(tid, id) do
        :reset -> {resets + 1, drops}
        :drop -> {resets, drops + 1}
        :skip -> {resets, drops}
      end
    end)
  end

  # Pass the exact row observed by the lookup to IngestEventQueue's CAS helpers, so an event
  # acked, deleted, or re-claimed between lookup and write is neither dropped nor reset (a
  # re-claimed row carries a newer claimed_at and will not match). The :reset/:drop/:skip
  # verdict is gated on the operation count so telemetry only reports rows actually changed.
  defp act_on_stale_event(tid, id) do
    case :ets.lookup(tid, id) do
      [{^id, :processing, %{retries: retries}, _size, _claim, _claimed_at} = row]
      when retries >= @max_stale_retries - 1 ->
        IngestEventQueue.drop_stale_event(tid, row)

      [{^id, :processing, %{retries: retries} = le, _size, _claim, _claimed_at} = row] ->
        IngestEventQueue.reset_stale_event(tid, row, %{le | retries: (retries || 0) + 1})

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
