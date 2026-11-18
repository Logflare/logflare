defmodule Logflare.ShutdownDrain do
  @moduledoc """
  Stops spool intake and drains queued events, spool uploads, and acknowledgments.
  Incomplete spool work is nacked for redelivery when the cleanup budget allows.
  """

  require Logger

  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.Spool.ConsumerPipeline
  alias Logflare.Backends.Spool.ConsumerPipeline.QueueProducer
  alias Logflare.Backends.Spool.DurableBuffer.Supervisor, as: SpoolDurableBufferSup
  alias Logflare.Backends.Spool.ObjectDeleter
  alias Logflare.Backends.Spool.SpoolAck
  alias Logflare.Shutdown.Budget
  alias Logflare.Utils.Tasks

  @default_poll_interval_ms 100

  @type result :: :drained | :timeout
  @type intake :: %{
          producers: (-> [GenServer.server()]),
          requested: MapSet.t(GenServer.server()),
          result: :ok | :timeout
        }

  @spec stop_receiving(integer(), (-> [GenServer.server()])) :: intake()
  def stop_receiving(deadline, producers \\ &spool_producers/0) do
    request_stops(
      producers.(),
      %{producers: producers, requested: MapSet.new(), result: :ok},
      deadline
    )
  end

  @spec spool_buffer() :: atom() | nil
  def spool_buffer do
    if SpoolDurableBufferSup.partitions() != [], do: SpoolDurableBufferSup.name()
  end

  @spec run(Budget.flush() | nil, intake(), atom() | nil, keyword()) :: result()
  def run(budget, intake, buffer, opts \\ [])

  def run(nil, %{result: :ok}, _buffer, _opts), do: :drained
  def run(nil, %{result: :timeout}, _buffer, _opts), do: :timeout

  def run(budget, intake, buffer, opts) do
    started_at = System.monotonic_time(:millisecond)
    queue_deadline = Budget.deadline(budget.queues)
    poll_interval_ms = Keyword.get(opts, :poll_interval_ms, @default_poll_interval_ms)

    Logger.warning(
      "shutdown_drain: started, requested intake stop on #{MapSet.size(intake.requested)} spool producer(s)"
    )

    queues = await_drained(intake, queue_deadline, poll_interval_ms)
    spool = sync_spool_buffer(buffer, Budget.deadline(budget.sync))
    spool_pending = SpoolAck.pending_count()
    queued_events = IngestEventQueue.total_size()
    {acks, object_deletes} = cleanup(queues, Budget.deadline(budget.cleanup))

    result =
      if queues == :drained and spool != :timeout and acks == :ok and object_deletes == :ok,
        do: :drained,
        else: :timeout

    Logger.warning(
      "shutdown_drain: finished result=#{result} spool_pending=#{spool_pending} " <>
        "queued_events=#{queued_events} duration_ms=#{System.monotonic_time(:millisecond) - started_at} " <>
        "spool_buffer=#{spool} object_deletes=#{object_deletes}"
    )

    result
  end

  defp cleanup(queue_result, deadline) do
    if queue_result == :timeout, do: within(deadline, &SpoolAck.nack_pending/1)

    acks = within(deadline, &SpoolAck.flush/1)
    {acks, within(deadline, &ObjectDeleter.flush/1)}
  end

  defp within(deadline, operation), do: operation.(Budget.remaining(deadline))

  defp sync_spool_buffer(nil, _deadline), do: :none

  defp sync_spool_buffer(buffer, deadline) do
    timeout = Budget.remaining(deadline)
    Logger.warning("shutdown_drain: syncing the spool buffer for up to #{timeout}ms")

    case Tasks.run_within(fn -> DurableBuffer.sync_all(buffer) end, timeout) do
      {:ok, :ok} -> :synced
      _ -> :timeout
    end
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

  defp request_stops(producers, intake, deadline) do
    Enum.reduce(producers, intake, fn producer, intake ->
      if MapSet.member?(intake.requested, producer) do
        intake
      else
        request_stop(producer, intake, deadline)
      end
    end)
  end

  defp request_stop(producer, intake, deadline) do
    result =
      producer_call(fn -> QueueProducer.stop_receiving(producer, Budget.remaining(deadline)) end)

    %{
      intake
      | requested: MapSet.put(intake.requested, producer),
        result: if(result == :timeout, do: :timeout, else: intake.result)
    }
  end

  defp await_drained(intake, deadline, poll_interval_ms) do
    producers = intake.producers.()
    intake = request_stops(producers, intake, deadline)

    cond do
      drained?(producers, deadline) and MapSet.new(intake.producers.()) == MapSet.new(producers) ->
        :drained

      Budget.remaining(deadline) == 0 ->
        :timeout

      true ->
        Process.sleep(min(poll_interval_ms, Budget.remaining(deadline)))
        await_drained(intake, deadline, poll_interval_ms)
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
