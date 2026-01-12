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
      consolidated_key: consolidated_key
    }

    handle_info(:work, state)
    {:ok, state}
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

  @spec drop_queue(map(), tuple(), pid() | nil, boolean(), :consolidated | :source) ::
          :ok | nil
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
