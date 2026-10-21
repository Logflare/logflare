defmodule Logflare.Backends.Spool.ConsumerDrain do
  @moduledoc """
  Drains a spool consumer node before it stops, so a pod shutdown doesn't
  strand queue messages until their visibility timeout expires.

  Run by `Logflare.SigtermHandler` before `System.stop/0`, while the backend
  pipelines are still running:

  1. Every `ConsumerPipeline` producer stops receiving queue messages
     (`QueueProducer.stop_fetching/1`); files already loaded keep flowing.
  2. Waits until every producer is drained and `SpoolAck` has no pending
     handles, i.e. every in-flight file reached its backends and was acked.
  3. At the deadline, nacks whatever is still pending so another consumer
     picks it up straight away.
  4. Flushes `SpoolAck` so queued acks reach the queue before shutdown.

  The deadline is `:consumer_drain_timeout_ms` in the `:spool` config
  (`SPOOL_CONSUMER_DRAIN_TIMEOUT_MS`), default 60 seconds. Keep the pod's
  termination grace period comfortably above it.
  """

  require Logger

  alias Logflare.Backends.Spool.ConsumerPipeline
  alias Logflare.Backends.Spool.ConsumerPipeline.QueueProducer
  alias Logflare.Backends.Spool.SpoolAck

  @default_timeout_ms 60_000
  @default_poll_interval_ms 100

  @type result :: :drained | :timeout

  @spec run(keyword()) :: result()
  def run(opts \\ []) do
    timeout_ms = Keyword.get_lazy(opts, :timeout_ms, &timeout_ms/0)
    poll_interval_ms = Keyword.get(opts, :poll_interval_ms, @default_poll_interval_ms)
    producers = Keyword.get_lazy(opts, :producers, &producers/0)
    started_at = System.monotonic_time(:millisecond)

    Enum.each(producers, &stop_fetching/1)

    Logger.warning(
      "spool_consumer: drain started, stopped fetching on #{length(producers)} producer(s), " <>
        "waiting up to #{timeout_ms}ms for in-flight files"
    )

    result = await_drained(producers, started_at + timeout_ms, poll_interval_ms)
    pending = SpoolAck.pending_count()

    if result == :timeout do
      Logger.warning("spool_consumer: drain timed out, nacking #{pending} in-flight files")
      SpoolAck.nack_pending()
    end

    SpoolAck.flush()

    duration = System.monotonic_time(:millisecond) - started_at

    Logger.warning(
      "spool_consumer: drain finished result=#{result} pending=#{pending} duration_ms=#{duration}"
    )

    :telemetry.execute(
      [:logflare, :backends, :spool, :consumer, :drain],
      %{duration: duration, pending: pending},
      %{result: result}
    )

    result
  end

  @spec timeout_ms() :: non_neg_integer()
  def timeout_ms do
    :logflare
    |> Application.get_env(:spool, [])
    |> Keyword.get(:consumer_drain_timeout_ms, @default_timeout_ms)
  end

  defp producers do
    if Process.whereis(ConsumerPipeline),
      do: Broadway.producer_names(ConsumerPipeline),
      else: []
  end

  defp await_drained(producers, deadline, poll_interval_ms) do
    cond do
      drained?(producers) ->
        :drained

      System.monotonic_time(:millisecond) >= deadline ->
        :timeout

      true ->
        Process.sleep(poll_interval_ms)
        await_drained(producers, deadline, poll_interval_ms)
    end
  end

  defp drained?(producers) do
    Enum.all?(producers, &producer_drained?/1) and SpoolAck.pending_count() == 0
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
