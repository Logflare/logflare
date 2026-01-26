defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.Pipeline do
  @moduledoc """
  Broadway pipeline for the ClickHouse adaptor.

  This pipeline is responsible for taking log events from the
  consolidated queue and inserting them into the backend's ingest table.
  Events from multiple sources are processed together in a single pipeline.
  """

  require Logger
  require OpenTelemetry.Tracer

  alias Broadway.Message
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.BufferProducer
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.LogEvent
  alias Logflare.Utils

  @producer_concurrency 1
  @processor_concurrency 4
  @batcher_concurrency 2
  @batch_size 50_000
  @batch_timeout 4_500
  @max_retries 0

  @doc false
  @spec max_retries() :: non_neg_integer()
  def max_retries, do: @max_retries

  @doc false
  @spec child_spec(arg :: term()) :: Supervisor.child_spec()
  def child_spec(arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]}
    }
  end

  @doc false
  @spec start_link(list()) ::
          {:ok, pid()} | :ignore | {:error, {:already_started, pid()} | term()}
  def start_link(args) do
    {name, args} = Keyword.pop(args, :name)
    backend = Keyword.fetch!(args, :backend)

    Broadway.start_link(__MODULE__,
      name: name,
      hibernate_after: 5_000,
      spawn_opt: [fullsweep_after: 10],
      producer: [
        module: {BufferProducer, [backend_id: backend.id, consolidated: true]},
        transformer: {__MODULE__, :transform, [backend_id: backend.id]},
        concurrency: @producer_concurrency
      ],
      processors: [
        default: [concurrency: @processor_concurrency, min_demand: 1, max_demand: 100]
      ],
      batchers: [
        ch: [
          concurrency: @batcher_concurrency,
          batch_size: @batch_size,
          batch_timeout: @batch_timeout
        ]
      ],
      context: %{backend_id: backend.id}
    )
  end

  @spec process_name(via_tuple :: {:via, module(), {module(), term()}}, base_name :: term()) ::
          {:via, module(), {module(), term()}}
  def process_name({:via, module, {registry, identifier}}, base_name) do
    new_identifier = Utils.append_to_tuple(identifier, base_name)
    {:via, module, {registry, new_identifier}}
  end

  @spec handle_message(processor_name :: atom(), message :: Message.t(), context :: map()) ::
          Message.t()
  def handle_message(_processor_name, message, _context) do
    Message.put_batcher(message, :ch)
  end

  @spec handle_batch(
          batcher :: atom(),
          messages :: [Message.t()],
          batch_info :: Broadway.BatchInfo.t(),
          context :: map()
        ) :: [Message.t()]
  def handle_batch(:ch, messages, batch_info, %{backend_id: backend_id}) do
    :telemetry.execute(
      [:logflare, :backends, :pipeline, :handle_batch],
      %{batch_size: batch_info.size, batch_trigger: batch_info.trigger},
      %{backend_type: :clickhouse, backend_id: backend_id}
    )

    result =
      OpenTelemetry.Tracer.with_span :clickhouse_pipeline, %{
        attributes: %{
          backend_id: backend_id,
          ingest_batch_size: batch_info.size,
          ingest_batch_trigger: batch_info.trigger
        }
      } do
        backend = Backends.Cache.get_backend(backend_id)
        events = Enum.map(messages, & &1.data)

        ClickHouseAdaptor.insert_log_events(backend, events)
      end

    case result do
      :ok ->
        messages

      {:error, reason} ->
        Enum.map(messages, &Message.failed(&1, reason))
    end
  end

  @spec transform(event :: LogEvent.t(), opts :: keyword()) :: Message.t()
  def transform(event, opts) do
    %Message{
      data: event,
      acknowledger: {__MODULE__, :ack_id, %{backend_id: opts[:backend_id]}}
    }
  end

  @spec ack(ack_ref :: term(), successful :: [Message.t()], failed :: [Message.t()]) :: :ok
  def ack(_ack_ref, _successful, []), do: :ok

  def ack(_ack_ref, _successful, failed) do
    failed
    |> Enum.group_by(fn %{acknowledger: {_, _, ack_data}} -> ack_data end)
    |> Enum.each(fn {%{backend_id: backend_id}, messages} ->
      {retriable, exhausted} =
        Enum.split_with(messages, fn %{data: event} ->
          (event.retries || 0) < @max_retries
        end)

      drop_exhausted_messages(exhausted, backend_id)
      requeue_retriable_messages(retriable, backend_id)
    end)
  end

  @spec drop_exhausted_messages(exhausted :: [Message.t()], backend_id :: pos_integer()) :: :ok
  defp drop_exhausted_messages([], _backend_id), do: :ok

  defp drop_exhausted_messages(exhausted, backend_id) do
    Logger.warning(
      "Dropping #{length(exhausted)} ClickHouse events after #{@max_retries} retries",
      backend_id: backend_id
    )

    events = Enum.map(exhausted, fn %{data: %LogEvent{} = event} -> event end)
    IngestEventQueue.delete_batch({:consolidated, backend_id}, events)
  end

  @spec requeue_retriable_messages(retriable :: [Message.t()], backend_id :: pos_integer()) :: :ok
  defp requeue_retriable_messages([], _backend_id), do: :ok

  defp requeue_retriable_messages(retriable, backend_id) do
    events =
      Enum.map(retriable, fn %{data: %LogEvent{} = event} ->
        %LogEvent{event | retries: (event.retries || 0) + 1}
      end)

    Logger.info(
      "Requeuing #{length(events)} ClickHouse events for retry",
      backend_id: backend_id
    )

    IngestEventQueue.delete_batch({:consolidated, backend_id}, events)
    IngestEventQueue.add_to_table({:consolidated, backend_id}, events)
  end
end
