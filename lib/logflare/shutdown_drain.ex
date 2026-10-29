defmodule Logflare.ShutdownDrain do
  @moduledoc """
  Drains a node before it stops, so a pod shutdown doesn't drop the events it
  still holds or strand spool queue messages until their visibility timeout.

  Spool producers stop receiving new queue messages, then the node waits for
  queued events and pending spool acks to clear, and for events accepted into
  the spool buffer to be uploaded. Anything still pending at the timeout is
  nacked so another consumer can pick it up.

  Keep the pod's termination grace period comfortably above the timeout.
  """

  require Logger

  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.Spool.ConsumerPipeline
  alias Logflare.Backends.Spool.ConsumerPipeline.QueueProducer
  alias Logflare.Backends.Spool.DurableBuffer.Supervisor, as: SpoolDurableBufferSup
  alias Logflare.Backends.Spool.SpoolAck
  alias Logflare.Utils.Tasks

  @default_poll_interval_ms 100
  @default_spool_sync_timeout_ms 10_000
  @default_cleanup_timeout_ms 10_000

  @type result :: :drained | :timeout

  @type opts :: [
          timeout_ms: non_neg_integer(),
          min_wait_ms: non_neg_integer(),
          poll_interval_ms: pos_integer(),
          producers: [GenServer.server()],
          spool_buffer: atom() | nil,
          spool_sync_timeout_ms: non_neg_integer(),
          cleanup_timeout_ms: non_neg_integer()
        ]

  @doc """
  Drains the node and returns `:drained` or `:timeout`.

  Options:

    * `:timeout_ms` - required. Maximum time to wait before nacking pending spool files.
    * `:min_wait_ms` - minimum time to wait before checking for drain completion, so load
      balancers stop sending traffic first. Defaults to `0`.
    * `:poll_interval_ms` - how often to check whether the node has drained. Defaults to `100`.
    * `:producers` - spool producers to stop receiving on. Defaults to every `ConsumerPipeline` producer.
    * `:spool_buffer` - spool producer `DurableBuffer` to sync. Defaults to this node's, if started.
    * `:spool_sync_timeout_ms` - how long to wait for the spool buffer to sync. Defaults to `10_000`.
    * `:cleanup_timeout_ms` - shared budget for nacking leftover spool files and flushing acks.
      Defaults to `10_000`.
  """
  @spec run(opts()) :: result()
  def run(opts) do
    timeout_ms = Keyword.fetch!(opts, :timeout_ms)
    min_wait_ms = Keyword.get(opts, :min_wait_ms, 0)
    poll_interval_ms = Keyword.get(opts, :poll_interval_ms, @default_poll_interval_ms)
    producers = Keyword.get_lazy(opts, :producers, &spool_producers/0)
    spool_buffer = Keyword.get_lazy(opts, :spool_buffer, &spool_buffer/0)

    spool_sync_timeout_ms =
      Keyword.get(opts, :spool_sync_timeout_ms, @default_spool_sync_timeout_ms)

    cleanup_timeout_ms = Keyword.get(opts, :cleanup_timeout_ms, @default_cleanup_timeout_ms)
    started_at = System.monotonic_time(:millisecond)

    Enum.each(producers, &stop_receiving/1)

    Logger.warning(
      "shutdown_drain: started, stopped receiving on #{length(producers)} spool producer(s), " <>
        "waiting at least #{min_wait_ms}ms and up to #{timeout_ms}ms for in-flight events"
    )

    wait_result =
      await_drained(
        producers,
        started_at + min_wait_ms,
        started_at + timeout_ms,
        poll_interval_ms
      )

    spool_buffer_result = sync_spool_buffer(spool_buffer, spool_sync_timeout_ms)
    spool_pending = SpoolAck.pending_count()
    queued_events = IngestEventQueue.total_size()
    cleanup_deadline = System.monotonic_time(:millisecond) + cleanup_timeout_ms

    if wait_result == :timeout, do: SpoolAck.nack_pending(cleanup_timeout_ms)

    SpoolAck.flush(max(cleanup_deadline - System.monotonic_time(:millisecond), 0))

    result =
      if wait_result == :drained and spool_buffer_result != :timeout, do: :drained, else: :timeout

    duration = System.monotonic_time(:millisecond) - started_at

    Logger.warning(
      "shutdown_drain: finished result=#{result} spool_pending=#{spool_pending} " <>
        "queued_events=#{queued_events} duration_ms=#{duration} spool_buffer=#{spool_buffer_result}"
    )

    result
  end

  defp spool_buffer do
    if SpoolDurableBufferSup.partitions() != [], do: SpoolDurableBufferSup.name()
  end

  defp sync_spool_buffer(nil, _timeout_ms), do: :none

  defp sync_spool_buffer(buffer, timeout_ms) do
    Logger.warning("shutdown_drain: syncing the spool buffer for up to #{timeout_ms}ms")

    case Tasks.run_within(fn -> DurableBuffer.sync_all(buffer) end, timeout_ms) do
      {:ok, :ok} -> :synced
      _ -> :timeout
    end
  end

  defp spool_producers do
    if Process.whereis(ConsumerPipeline),
      do: Broadway.producer_names(ConsumerPipeline),
      else: []
  end

  defp await_drained(producers, earliest, deadline, poll_interval_ms) do
    now = System.monotonic_time(:millisecond)

    cond do
      now >= earliest and drained?(producers) ->
        :drained

      now >= deadline ->
        :timeout

      true ->
        Process.sleep(poll_interval_ms)
        await_drained(producers, earliest, deadline, poll_interval_ms)
    end
  end

  defp drained?(producers) do
    Enum.all?(producers, &producer_drained?/1) and SpoolAck.pending_count() == 0 and
      IngestEventQueue.total_size() == 0
  end

  # A producer that has already exited settles its own prefetch (see
  # QueueProducer's deliver_or_settle/5), so there is nothing to stop.
  defp stop_receiving(producer) do
    QueueProducer.stop_receiving(producer)
  catch
    :exit, _reason -> :ok
  end

  defp producer_drained?(producer) do
    QueueProducer.drained?(producer)
  catch
    :exit, _reason -> true
  end
end
