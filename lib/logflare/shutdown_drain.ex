defmodule Logflare.ShutdownDrain do
  @moduledoc """
  Stops spool intake and drains queued events, spool commits, and acknowledgments.
  Incomplete spool work is nacked for redelivery when the cleanup budget allows.
  An empty ingestion queue doesn't confirm delivery of claimed direct-backend events.
  A local WAL commit doesn't confirm upload and queue publication.
  """

  require Logger

  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.Spool.ConsumerPipeline
  alias Logflare.Backends.Spool.ConsumerPipeline.QueueProducer
  alias Logflare.Backends.Spool.DurableBuffer.Backends.RotatingWal
  alias Logflare.Backends.Spool.DurableBuffer.Supervisor, as: SpoolDurableBufferSup
  alias Logflare.Backends.Spool.ObjectDeleter
  alias Logflare.Backends.Spool.SpoolAck
  alias Logflare.Shutdown.Budget

  @default_poll_interval_ms 100

  @type result :: :drained | :timeout | :error
  @type intake_result :: :ok | :timeout
  @type spool_sync_result ::
          :none | :synced | :local_wal_synced_cloud_delivery_unverified | :timeout | :error

  @doc """
  Requests prefetch settlement on current producers after readiness enters draining.
  Replacement producers start with intake closed and do not need a stop request.
  """
  @spec stop_receiving(integer(), [GenServer.server()]) :: intake_result()
  def stop_receiving(deadline, producers \\ spool_producers()) do
    Logger.warning(
      "shutdown_drain: requesting intake stop on #{length(producers)} spool producer(s)"
    )

    Enum.reduce(producers, :ok, fn producer, status ->
      result =
        producer_call(fn ->
          QueueProducer.stop_receiving(producer, Budget.remaining(deadline))
        end)

      if result == :timeout, do: :timeout, else: status
    end)
  end

  @spec spool_buffer() :: atom() | nil
  def spool_buffer do
    if SpoolDurableBufferSup.partitions() != [], do: SpoolDurableBufferSup.name()
  end

  @spec run(Budget.flush() | nil, intake_result(), atom() | nil, keyword()) :: result()
  def run(budget, intake_result, buffer, opts \\ [])

  def run(nil, :ok, _buffer, _opts), do: :drained
  def run(nil, :timeout, _buffer, _opts), do: :timeout

  def run(budget, _intake_result, buffer, opts) do
    started_at = System.monotonic_time(:millisecond)
    queue_deadline = Budget.deadline(budget.queues)
    poll_interval_ms = Keyword.get(opts, :poll_interval_ms, @default_poll_interval_ms)
    producers = Keyword.get(opts, :producers, &spool_producers/0)

    Logger.warning("shutdown_drain: started")
    queues = await_drained(producers, queue_deadline, poll_interval_ms)
    spool = sync_spool_buffer(budget.sync, buffer)
    spool_pending = SpoolAck.pending_count()
    queued_events = IngestEventQueue.total_size()
    {acks, object_deletes} = cleanup(queues, budget.cleanup)

    result =
      cond do
        spool == :error -> :error
        :timeout in [queues, spool, acks, object_deletes] -> :timeout
        true -> :drained
      end

    Logger.warning(
      "shutdown_drain: finished result=#{result} spool_pending=#{spool_pending} " <>
        "queued_events=#{queued_events} duration_ms=#{System.monotonic_time(:millisecond) - started_at} " <>
        "spool_buffer=#{spool} object_deletes=#{object_deletes} direct_backend_delivery=unverified"
    )

    result
  end

  @doc "Settles remaining spool receipts and attempts outstanding acknowledgments and deletes."
  @spec cleanup(:drained | :timeout, Budget.limit()) :: {:ok | :timeout, :ok | :timeout}
  def cleanup(queue_result, limit) do
    deadline = Budget.deadline(limit)
    if queue_result == :timeout, do: within(deadline, &SpoolAck.nack_pending/1)

    acks = within(deadline, &SpoolAck.flush/1)
    {acks, within(deadline, &ObjectDeleter.flush/1)}
  end

  defp within(deadline, operation), do: operation.(Budget.remaining(deadline))

  @spec sync_spool_buffer(Budget.limit() | nil, atom() | nil) :: spool_sync_result()
  def sync_spool_buffer(nil, _buffer), do: :none
  def sync_spool_buffer(_limit, nil), do: :none

  def sync_spool_buffer(limit, buffer) do
    deadline = Budget.deadline(limit)

    Logger.warning(
      "shutdown_drain: syncing the spool buffer for up to #{Budget.remaining(deadline)}ms"
    )

    sync_buffer(buffer, deadline)
  end

  defp sync_buffer(buffer, deadline) do
    %{backend: {backend, _}} = DurableBuffer.config(buffer)

    case DurableBuffer.sync_all(buffer, Budget.remaining(deadline)) do
      :ok when backend == RotatingWal -> :local_wal_synced_cloud_delivery_unverified
      :ok -> :synced
      {:error, failures} -> report_sync_failures(failures)
    end
  rescue
    ArgumentError ->
      Logger.warning("shutdown_drain: spool buffer unavailable during sync")
      :error
  end

  defp report_sync_failures(failures) do
    {timed_out, failed} = Enum.split_with(failures, fn {_index, reason} -> reason == :timeout end)

    Logger.warning(
      "shutdown_drain: spool sync incomplete failed_partitions=#{inspect(Enum.map(failed, &elem(&1, 0)))} " <>
        "timed_out_partitions=#{inspect(Enum.map(timed_out, &elem(&1, 0)))}"
    )

    if failed == [], do: :timeout, else: :error
  end

  defp spool_producers do
    if Process.whereis(ConsumerPipeline) do
      for name <- Broadway.producer_names(ConsumerPipeline),
          pid = GenServer.whereis(name),
          do: pid
    else
      []
    end
  catch
    :exit, {:noproc, {Broadway.Topology, :config, _}} -> []
  end

  defp await_drained(producers, deadline, poll_interval_ms) do
    cond do
      drained?(producers.(), deadline) ->
        :drained

      Budget.remaining(deadline) == 0 ->
        :timeout

      true ->
        Process.sleep(min(poll_interval_ms, Budget.remaining(deadline)))
        await_drained(producers, deadline, poll_interval_ms)
    end
  end

  defp drained?(producers, deadline) do
    Enum.all?(producers, fn producer ->
      producer_call(fn -> QueueProducer.drained?(producer, Budget.remaining(deadline)) end) in [
        true,
        :gone
      ]
    end) and SpoolAck.pending_count() == 0 and IngestEventQueue.total_size() == 0
  end

  defp producer_call(fun) do
    fun.()
  catch
    :exit, {:noproc, _call} -> :gone
    :exit, _reason -> :timeout
  end
end
