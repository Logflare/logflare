defmodule Logflare.ShutdownDrain do
  @moduledoc """
  Drains a node before it stops, so a pod shutdown doesn't drop the events it
  still holds or strand spool queue messages until their visibility timeout.

  Spool producers stop receiving new queue messages, then the node waits for
  queued events and pending spool acks to clear. Anything still pending at the
  timeout is nacked so another consumer can pick it up.

  Keep the pod's termination grace period comfortably above the timeout.
  """

  require Logger

  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.Spool.ConsumerPipeline
  alias Logflare.Backends.Spool.ConsumerPipeline.QueueProducer
  alias Logflare.Backends.Spool.SpoolAck

  @default_poll_interval_ms 100

  @type result :: :drained | :timeout

  @type opts :: [
          timeout_ms: non_neg_integer(),
          min_wait_ms: non_neg_integer(),
          poll_interval_ms: pos_integer(),
          producers: [GenServer.server()]
        ]

  @doc """
  Drains the node and returns `:drained` or `:timeout`.

  Options:

    * `:timeout_ms` - required. Maximum time to wait before nacking pending spool files.
    * `:min_wait_ms` - minimum time to wait before checking for drain completion, so load
      balancers stop sending traffic first. Defaults to `0`.
    * `:poll_interval_ms` - how often to check whether the node has drained. Defaults to `100`.
    * `:producers` - spool producers to stop receiving on. Defaults to every `ConsumerPipeline` producer.
  """
  @spec run(opts()) :: result()
  def run(opts) do
    timeout_ms = Keyword.fetch!(opts, :timeout_ms)
    min_wait_ms = Keyword.get(opts, :min_wait_ms, 0)
    poll_interval_ms = Keyword.get(opts, :poll_interval_ms, @default_poll_interval_ms)
    producers = Keyword.get_lazy(opts, :producers, &spool_producers/0)
    started_at = System.monotonic_time(:millisecond)

    Enum.each(producers, &stop_receiving/1)

    Logger.warning(
      "shutdown_drain: started, stopped receiving on #{length(producers)} spool producer(s), " <>
        "waiting at least #{min_wait_ms}ms and up to #{timeout_ms}ms for in-flight events"
    )

    result = await_drained(started_at + min_wait_ms, started_at + timeout_ms, poll_interval_ms)

    spool_pending = SpoolAck.pending_count()
    queued_events = IngestEventQueue.total_size()

    if result == :timeout, do: SpoolAck.nack_pending()

    SpoolAck.flush()

    duration = System.monotonic_time(:millisecond) - started_at

    Logger.warning(
      "shutdown_drain: finished result=#{result} spool_pending=#{spool_pending} " <>
        "queued_events=#{queued_events} duration_ms=#{duration}"
    )

    result
  end

  defp spool_producers do
    if Process.whereis(ConsumerPipeline),
      do: Broadway.producer_names(ConsumerPipeline),
      else: []
  end

  defp await_drained(earliest, deadline, poll_interval_ms) do
    now = System.monotonic_time(:millisecond)

    cond do
      now >= earliest and drained?() ->
        :drained

      now >= deadline ->
        :timeout

      true ->
        Process.sleep(poll_interval_ms)
        await_drained(earliest, deadline, poll_interval_ms)
    end
  end

  defp drained?, do: SpoolAck.pending_count() == 0 and IngestEventQueue.total_size() == 0

  # A producer that has already exited settles its own prefetch (see
  # QueueProducer's deliver_or_settle/5), so there is nothing to stop.
  defp stop_receiving(producer) do
    QueueProducer.stop_receiving(producer)
  catch
    :exit, _reason -> :ok
  end
end
