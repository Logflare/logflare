defmodule Logflare.Backends.Adaptor.BigQueryV2Adaptor.Pipeline do
  @moduledoc """
  Broadway pipeline for `BigQueryV2Adaptor`, consuming the consolidated per-backend
  queue (all sources of a backend share one pipeline).

  Processors map each event to the OTEL column format as an NDJSON row with
  `Logflare.Mapper`. Batches are keyed by event type so each append targets a single
  table (`otel_logs`, ...); the rows are encoded to Arrow against the table's fixed
  schema and appended through the Storage Write API.

  Events of a type without a table, or that fail to map, are dropped immediately.
  Failed appends are requeued with a bounded retry counter and dropped once retries
  are exhausted.
  """

  @behaviour Broadway
  @behaviour Broadway.Acknowledger

  import Logflare.Utils.Guards, only: [is_event_type: 1]

  require Logger

  alias Broadway.Message
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.BigQueryV2Adaptor
  alias Logflare.Backends.Adaptor.BigQueryV2Adaptor.TableSchema
  alias Logflare.Backends.BufferProducer
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.LogEvent
  alias Logflare.LogEvent.TypeDetection
  alias Logflare.Mapper
  alias Logflare.Mapper.ConfigStore
  alias Logflare.Mapper.OutputContext
  alias Logflare.Utils

  @producer_concurrency 1
  @processor_concurrency 4
  @batcher_concurrency 2
  @batch_size 500
  @batch_timeout 2_000
  @max_retries 1

  @doc false
  @spec max_retries() :: non_neg_integer()
  def max_retries, do: @max_retries

  @doc false
  def child_spec(args) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [args]}
    }
  end

  @doc false
  @spec start_link(keyword()) :: {:ok, pid()} | :ignore | {:error, term()}
  def start_link(args) when is_list(args) do
    {name, args} = Keyword.pop(args, :name)
    backend = Keyword.fetch!(args, :backend)

    Broadway.start_link(__MODULE__,
      name: name,
      hibernate_after: 5_000,
      producer: [
        module: {BufferProducer, [backend_id: backend.id, consolidated: true]},
        transformer: {__MODULE__, :transform, [backend_id: backend.id]},
        concurrency: @producer_concurrency
      ],
      processors: [
        default: [concurrency: @processor_concurrency, min_demand: 1]
      ],
      batchers: [
        bigquery: [
          concurrency: @batcher_concurrency,
          batch_size: @batch_size,
          batch_timeout: @batch_timeout
        ]
      ],
      context: build_context(backend.id)
    )
  end

  @impl Broadway
  def process_name({:via, module, {registry, identifier}}, base_name) do
    {:via, module, {registry, Utils.append_to_tuple(identifier, base_name)}}
  end

  @doc false
  @spec transform(LogEvent.t(), keyword()) :: Message.t()
  def transform(%LogEvent{} = event, opts) do
    %Message{
      data: event,
      acknowledger: {__MODULE__, :ack_id, %{backend_id: opts[:backend_id]}}
    }
  end

  @impl Broadway
  def handle_message(
        _processor_name,
        %Message{data: %LogEvent{event_type: event_type} = event} = message,
        %{mapper_configs: mapper_configs}
      )
      when is_map_key(mapper_configs, event_type) do
    %{compiled: compiled, config_id: config_id} = Map.fetch!(mapper_configs, event_type)
    output_context = OutputContext.ndjson(event, config_id)

    case Mapper.map_result(event.body, compiled, output_context: output_context) do
      {:ok, row} ->
        message
        |> Message.put_batcher(:bigquery)
        |> Message.put_batch_key(event_type)
        |> Message.put_data({event, row})

      {:error, reason} ->
        Message.failed(message, {:map_failed, reason})
    end
  end

  def handle_message(_processor_name, %Message{data: %LogEvent{} = event} = message, _context) do
    Message.failed(message, {:unsupported_event_type, event.event_type})
  end

  @impl Broadway
  def handle_batch(:bigquery, messages, %{batch_key: event_type} = batch_info, %{
        backend_id: backend_id
      })
      when is_event_type(event_type) do
    emit_batch_telemetry(batch_info, backend_id, event_type)

    backend = Backends.Cache.get_backend(backend_id)
    rows = Enum.map(messages, fn %Message{data: {_event, row}} -> row end)

    case BigQueryV2Adaptor.insert_rows(backend, event_type, rows) do
      :ok ->
        messages

      {:error, reason} ->
        Logger.warning("BigQuery v2 append failed",
          backend_id: backend_id,
          event_type: event_type,
          error_string: inspect(reason)
        )

        Enum.map(messages, &Message.failed(&1, reason))
    end
  end

  @impl Broadway.Acknowledger
  def ack(_ack_ref, _successful, []), do: :ok

  def ack(_ack_ref, _successful, failed) do
    failed
    |> Enum.group_by(fn %Message{acknowledger: {_, _, %{backend_id: backend_id}}} ->
      backend_id
    end)
    |> Enum.each(fn {backend_id, messages} -> handle_failures(backend_id, messages) end)
  end

  @spec build_context(pos_integer()) :: map()
  defp build_context(backend_id) do
    mapper_configs =
      Map.new(TableSchema.event_types(), fn event_type ->
        {:ok, compiled, config_id} = ConfigStore.get_compiled(event_type, :ndjson)
        {event_type, %{compiled: compiled, config_id: config_id}}
      end)

    %{backend_id: backend_id, mapper_configs: mapper_configs}
  end

  @spec handle_failures(pos_integer(), [Message.t()]) :: :ok
  defp handle_failures(backend_id, messages) do
    {retriable, dropped} =
      Enum.split_with(messages, fn %Message{status: {:failed, reason}} = message ->
        retriable?(reason) and event_of(message).retries < @max_retries
      end)

    dropped
    |> Enum.group_by(fn %Message{status: {:failed, reason}} -> drop_reason(reason) end)
    |> Enum.each(fn {reason, messages} ->
      Logger.warning("Dropping #{length(messages)} BigQuery v2 events: #{reason}",
        backend_id: backend_id
      )
    end)

    requeue(backend_id, retriable)
  end

  @spec retriable?(term()) :: boolean()
  defp retriable?({:map_failed, _reason}), do: false
  defp retriable?({:unsupported_event_type, _event_type}), do: false
  defp retriable?({:encode_failed, _message}), do: false
  defp retriable?(_reason), do: true

  @spec drop_reason(term()) :: String.t()
  defp drop_reason({:map_failed, _reason}), do: "mapping failed"
  defp drop_reason({:unsupported_event_type, event_type}), do: "no table for #{event_type} events"
  defp drop_reason({:encode_failed, _message}), do: "Arrow encoding failed"
  defp drop_reason(_reason), do: "exhausted #{@max_retries} retries"

  @spec requeue(pos_integer(), [Message.t()]) :: :ok
  defp requeue(_backend_id, []), do: :ok

  defp requeue(backend_id, messages) do
    events =
      Enum.map(messages, fn message ->
        event = event_of(message)
        %{event | retries: event.retries + 1}
      end)

    Logger.info("Requeuing #{length(events)} BigQuery v2 events for retry",
      backend_id: backend_id
    )

    IngestEventQueue.add_to_table({:consolidated, backend_id}, events)
  end

  @spec event_of(Message.t()) :: LogEvent.t()
  defp event_of(%Message{data: {%LogEvent{} = event, _row}}), do: event
  defp event_of(%Message{data: %LogEvent{} = event}), do: event

  @spec emit_batch_telemetry(Broadway.BatchInfo.t(), pos_integer(), TypeDetection.event_type()) ::
          :ok
  defp emit_batch_telemetry(batch_info, backend_id, event_type) do
    :telemetry.execute(
      [:logflare, :backends, :pipeline, :handle_batch],
      %{batch_size: batch_info.size, batch_trigger: batch_info.trigger},
      %{
        backend_type: :bigquery_v2,
        backend_id: backend_id,
        event_type: event_type,
        batch_trigger: batch_info.trigger
      }
    )
  end
end
