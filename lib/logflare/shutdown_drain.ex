defmodule Logflare.ShutdownDrain do
  @moduledoc """
  Drains a node before it stops, so a pod shutdown doesn't drop the events it
  still holds or strand spool queue messages until their visibility timeout.

  Run by `Logflare.SigtermHandler` before `System.stop/0`, while every
  pipeline is still running, when `:shutdown_drain_timeout_ms` is configured
  (`LOGFLARE_SHUTDOWN_DRAIN_TIMEOUT_MS`):

  1. Every spool `ConsumerPipeline` producer stops receiving queue messages
     (`QueueProducer.stop_fetching/1`); files already loaded keep flowing.
  2. Waits at least `:min_wait_ms` (the SIGTERM grace period, so load
     balancers stop sending first), then until every spool producer is
     drained, `SpoolAck` has no pending handles and `IngestEventQueue` holds
     no events, i.e. everything reached its backends or the spool.
  3. At `:timeout_ms`, nacks spool files still pending so another consumer
     picks them up straight away.
  4. Flushes `SpoolAck` so queued acks reach the queue before shutdown.

  Keep the pod's termination grace period comfortably above the timeout.
  """

  require Logger

  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.Spool.ConsumerPipeline
  alias Logflare.Backends.Spool.ConsumerPipeline.QueueProducer
  alias Logflare.Backends.Spool.SpoolAck

  @default_poll_interval_ms 100

  @type result :: :drained | :timeout

  @spec run(keyword()) :: result()
  def run(opts) do
    timeout_ms = Keyword.fetch!(opts, :timeout_ms)
    min_wait_ms = Keyword.get(opts, :min_wait_ms, 0)
    poll_interval_ms = Keyword.get(opts, :poll_interval_ms, @default_poll_interval_ms)
    producers = Keyword.get_lazy(opts, :producers, &spool_producers/0)
    started_at = System.monotonic_time(:millisecond)

    Enum.each(producers, &stop_fetching/1)

    Logger.warning(
      "shutdown_drain: started, stopped fetching on #{length(producers)} spool producer(s), " <>
        "waiting at least #{min_wait_ms}ms and up to #{timeout_ms}ms for in-flight events"
    )

    result =
      await_drained(
        producers,
        started_at + min_wait_ms,
        started_at + timeout_ms,
        poll_interval_ms
      )

    spool_pending = SpoolAck.pending_count()
    queued_events = IngestEventQueue.total_size()

    if result == :timeout and spool_pending > 0 do
      Logger.warning("shutdown_drain: timed out, nacking #{spool_pending} in-flight spool files")
      SpoolAck.nack_pending()
    end

    SpoolAck.flush()

    duration = System.monotonic_time(:millisecond) - started_at

    Logger.warning(
      "shutdown_drain: finished result=#{result} spool_pending=#{spool_pending} " <>
        "queued_events=#{queued_events} duration_ms=#{duration}"
    )

    :telemetry.execute(
      [:logflare, :shutdown, :drain],
      %{duration: duration, spool_pending: spool_pending, queued_events: queued_events},
      %{result: result}
    )

    result
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
  # QueueProducer's deliver_or_settle/5), so it has nothing left to drain.
  defp stop_fetching(producer) do
    QueueProducer.stop_fetching(producer)
  catch
    :exit, _reason -> :ok
  end

  defp producer_drained?(producer) do
    QueueProducer.drained?(producer)
  catch
    :exit, _reason -> true
  end
end
