defmodule Logflare.Backends.Adaptor.S3TablesAdaptor.Pipeline do
  @moduledoc """
  Broadway pipeline for `S3TablesAdaptor`, consuming the consolidated
  per-backend queue (all sources of a backend share one pipeline).

  Events are batched by `{event_type, day_bucket}` so each Iceberg append
  targets a single type-specific table (otel_logs, otel_metrics, otel_traces)
  and, in the common case, a single day partition. Batches are flattened to
  the OTEL column format via `Logflare.Mapper`, NDJSON-encoded, and appended
  through `Native.append_batch/3`, which commits one Iceberg snapshot per
  batch. Failed batches are requeued with a bounded retry counter and dropped
  once retries are exhausted.
  """

  import Logflare.Utils.Guards, only: [is_event_type: 1]

  require Logger

  alias Broadway.Message
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.MappingConfigStore
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.CatalogManager
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.IcebergSchema
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.Native
  alias Logflare.Backends.BufferProducer
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.Pipeline.BatchSplitter
  alias Logflare.LogEvent
  alias Logflare.LogEvent.TypeDetection
  alias Logflare.Mapper
  alias Logflare.Mapper.PostProcess
  alias Logflare.Utils

  @behaviour Broadway
  @behaviour Broadway.Acknowledger

  @producer_concurrency 1
  @processor_concurrency 5

  @max_retries 2

  @doc false
  @spec max_retries() :: non_neg_integer()
  def max_retries, do: @max_retries

  @doc false
  def child_spec(arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]}
    }
  end

  @doc false
  @spec start_link(Keyword.t()) ::
          {:ok, pid()} | :ignore | {:error, {:already_started, pid()} | term()}
  def start_link(args) when is_list(args) do
    with {name, args} <- Keyword.pop(args, :name),
         backend <- Keyword.fetch!(args, :backend),
         batch_timeout <- Keyword.fetch!(args, :batch_timeout) do
      Broadway.start_link(__MODULE__,
        name: name,
        hibernate_after: 5_000,
        spawn_opt: [
          fullsweep_after: 10
        ],
        producer: [
          module: {BufferProducer, [backend_id: backend.id, consolidated: true]},
          transformer: {__MODULE__, :transform, [backend_id: backend.id]},
          concurrency: @producer_concurrency
        ],
        processors: [
          default: [concurrency: @processor_concurrency, min_demand: 1]
        ],
        batchers: [
          s3_tables: [
            concurrency: 1,
            batch_size: BatchSplitter.build(),
            max_demand: BatchSplitter.max_batch_size(),
            batch_timeout: batch_timeout
          ]
        ],
        context: %{backend_id: backend.id}
      )
    end
  end

  @impl Broadway
  def process_name({:via, module, {registry, identifier}}, base_name) do
    new_identifier = Utils.append_to_tuple(identifier, base_name)
    {:via, module, {registry, new_identifier}}
  end

  @impl Broadway
  def handle_message(
        _processor_name,
        %Message{data: %LogEvent{event_type: event_type, day_bucket: day_bucket}} = message,
        _context
      )
      when is_event_type(event_type) do
    message
    |> Message.put_batcher(:s3_tables)
    |> Message.put_batch_key({event_type, day_bucket})
  end

  @impl Broadway
  def handle_batch(
        :s3_tables,
        messages,
        %{batch_key: {event_type, day_bucket}} = batch_info,
        %{backend_id: backend_id}
      )
      when is_event_type(event_type) do
    emit_batch_telemetry(batch_info, backend_id, event_type, day_bucket)

    case append_batch(backend_id, messages, event_type) do
      {:ok, _info} ->
        messages

      {:error, reason} ->
        Logger.warning("S3 Tables append failed",
          backend_id: backend_id,
          event_type: event_type,
          error_string: inspect(reason)
        )

        Enum.map(messages, &Message.failed(&1, reason))
    end
  end

  @doc false
  @spec transform(event :: LogEvent.t(), opts :: keyword()) :: Message.t()
  def transform(event, opts) do
    %Message{
      data: event,
      acknowledger: {__MODULE__, :ack_id, %{backend_id: opts[:backend_id]}}
    }
  end

  @impl Broadway.Acknowledger
  def ack(_ack_ref, _successful, []), do: :ok

  def ack(_ack_ref, _successful, failed) do
    failed
    |> Enum.group_by(fn %{acknowledger: {_, _, ack_data}} -> ack_data end)
    |> Enum.each(fn {%{backend_id: backend_id}, messages} ->
      ack_backend_failures(backend_id, messages)
    end)
  end

  @spec append_batch(pos_integer(), [Message.t()], TypeDetection.event_type()) ::
          {:ok, map()} | {:error, term()}
  defp append_batch(backend_id, messages, event_type) do
    with {:ok, catalog} <- CatalogManager.fetch_catalog(backend_id),
         {:ok, compiled, config_id} <- MappingConfigStore.get_compiled(event_type) do
      ndjson = encode_batch(messages, compiled, config_id, event_type)
      table_name = IcebergSchema.table_name(event_type)

      {duration_us, result} =
        :timer.tc(fn -> Native.append_batch(catalog, table_name, ndjson) end)

      emit_append_telemetry(result, duration_us, backend_id, event_type)
      result
    end
  end

  @spec encode_batch([Message.t()], term(), String.t(), TypeDetection.event_type()) :: binary()
  defp encode_batch(messages, compiled, config_id, event_type) do
    messages
    |> Enum.map(fn %{data: %LogEvent{} = event} ->
      row =
        event.body
        |> Mapper.map(compiled)
        |> Map.put("mapping_config_id", config_id)
        |> PostProcess.apply(event_type)
        # like ClickHouse, these columns come from the struct, not the mapper
        |> Map.merge(%{
          "id" => event.id,
          "source_uuid" => event.source_uuid && Atom.to_string(event.source_uuid),
          "source_name" => event.source_name,
          "ingested_at" => event.ingested_at
        })

      [Jason.encode_to_iodata!(row), ?\n]
    end)
    |> IO.iodata_to_binary()
  end

  @spec ack_backend_failures(backend_id :: pos_integer(), messages :: [Message.t()]) :: :ok
  defp ack_backend_failures(backend_id, messages) do
    {retriable, exhausted} =
      Enum.split_with(messages, fn %{data: event} ->
        (event.retries || 0) < @max_retries
      end)

    drop_messages(exhausted, backend_id, "exhausted #{@max_retries} retries")
    requeue_retriable_messages(retriable, backend_id)
  end

  @spec drop_messages([Message.t()], pos_integer(), String.t()) :: :ok
  defp drop_messages([], _backend_id, _reason), do: :ok

  defp drop_messages(messages, backend_id, reason) do
    Logger.warning(
      "Dropping #{length(messages)} S3 Tables events: #{reason}",
      backend_id: backend_id
    )

    events = Enum.map(messages, fn %{data: %LogEvent{} = event} -> event end)

    try do
      IngestEventQueue.delete_batch({:consolidated, backend_id}, events)
    rescue
      ArgumentError -> :ok
    end
  end

  @spec requeue_retriable_messages([Message.t()], pos_integer()) :: :ok
  defp requeue_retriable_messages([], _backend_id), do: :ok

  defp requeue_retriable_messages(retriable, backend_id) do
    events =
      Enum.map(retriable, fn %{data: %LogEvent{} = event} ->
        %LogEvent{event | retries: (event.retries || 0) + 1}
      end)

    Logger.info(
      "Requeuing #{length(events)} S3 Tables events for retry",
      backend_id: backend_id
    )

    IngestEventQueue.delete_batch({:consolidated, backend_id}, events)
    IngestEventQueue.add_to_table({:consolidated, backend_id}, events)
  end

  @spec emit_batch_telemetry(
          Broadway.BatchInfo.t(),
          pos_integer(),
          TypeDetection.event_type(),
          integer()
        ) :: :ok
  defp emit_batch_telemetry(batch_info, backend_id, event_type, day_bucket) do
    :telemetry.execute(
      [:logflare, :backends, :pipeline, :handle_batch],
      %{batch_size: batch_info.size, batch_trigger: batch_info.trigger},
      %{
        backend_type: :s3_tables,
        backend_id: backend_id,
        event_type: event_type,
        day_bucket: day_bucket
      }
    )
  end

  @spec emit_append_telemetry(
          {:ok, map()} | {:error, term()},
          non_neg_integer(),
          pos_integer(),
          TypeDetection.event_type()
        ) :: :ok
  defp emit_append_telemetry(result, duration_us, backend_id, event_type) do
    {measurements, metadata} =
      case result do
        {:ok, %{row_count: row_count, data_files: data_files}} ->
          {%{duration_us: duration_us, row_count: row_count, data_files: data_files},
           %{status: :ok}}

        {:error, reason} ->
          {%{duration_us: duration_us},
           %{status: :error, reason: if(is_atom(reason), do: reason, else: :append_failed)}}
      end

    :telemetry.execute(
      [:logflare, :backends, :s3_tables, :append],
      measurements,
      Map.merge(metadata, %{backend_id: backend_id, event_type: event_type})
    )
  end
end
