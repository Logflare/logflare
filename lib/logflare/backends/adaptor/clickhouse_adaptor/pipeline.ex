defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.Pipeline do
  @moduledoc """
  Broadway pipeline for the ClickHouse adaptor.

  This pipeline is responsible for taking log events from the
  consolidated queue and inserting them into the backend's type-specific
  ingest tables (otel_logs, otel_metrics, otel_traces).

  Events are batched by a composite key of `{event_type, day_bucket}` so each
  insert targets a single ClickHouse partition.

  Uses ID-passing: the producer emits `LogEventPointer`s (id + routing metadata)
  while full events live in a separate generation store (see
  `Logflare.Backends.IngestEventQueue`). Processors resolve and replace each full event
  with a fused mapper-produced `EncodedRow`; batch processors only stream those
  reference-counted RowBinary rows through gzip and insert the compressed payload.
  """

  @behaviour Broadway.Acknowledger

  import Logflare.Utils.Guards, only: [is_event_type: 1, is_pos_integer: 1]

  require Logger
  require OpenTelemetry.Tracer

  alias Broadway.Message
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.CircuitBreaker
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.EncodedRow
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.Ingester
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.MappingConfigStore
  alias Logflare.Backends.Backend
  alias Logflare.Backends.BufferProducer
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.IngestEventQueue.LogEventPointer
  alias Logflare.LogEvent
  alias Logflare.LogEvent.TypeDetection
  alias Logflare.Mapper
  alias Logflare.Mapper.OutputContext
  alias Logflare.Utils

  @event_types [:log, :metric, :trace]
  @producer_concurrency 1
  @min_processor_concurrency 6
  @processor_min_demand 100
  @processor_max_demand 1_000
  @batch_size 60_000
  @batch_timeout 5_000
  # Bound each backend to four concurrent gzip/HTTP inserts. Higher concurrency can
  # monopolize the shared Finch pools and amplify ClickHouse timeouts under backlog.
  @batcher_concurrency 4
  @max_retries 1
  # Keep buffer capacity independent of concurrent gzip/HTTP inserts. This preserves
  # the previous 64-batch ceiling while four batch processors bound downstream load.
  # It remains a generous safety valve rather than fine-grained flow control — see
  # BufferProducer.capped_fetch_amount/2.
  @max_in_flight_batches 64
  @max_in_flight @batch_size * @max_in_flight_batches

  @doc false
  @spec processor_concurrency() :: pos_integer()
  def processor_concurrency, do: processor_concurrency(System.schedulers_online())

  @doc false
  @spec processor_concurrency(pos_integer()) :: pos_integer()
  def processor_concurrency(schedulers_online)
      when is_integer(schedulers_online) and schedulers_online > 0 do
    max(schedulers_online - @batcher_concurrency, @min_processor_concurrency)
  end

  @doc false
  @spec max_retries() :: non_neg_integer()
  def max_retries, do: @max_retries

  @doc false
  @spec max_batch_size() :: pos_integer()
  def max_batch_size, do: @batch_size

  @doc false
  @spec max_in_flight() :: pos_integer()
  def max_in_flight, do: @max_in_flight

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
    processor_concurrency = processor_concurrency()

    Broadway.start_link(__MODULE__,
      name: name,
      hibernate_after: 5_000,
      spawn_opt: [fullsweep_after: 100],
      producer: [
        module:
          {BufferProducer,
           [
             backend_id: backend.id,
             consolidated: true,
             id_passing: true,
             max_in_flight: @max_in_flight,
             seed_batch_size: @batch_size
           ]},
        transformer: {__MODULE__, :transform, [backend_id: backend.id]},
        concurrency: @producer_concurrency
      ],
      processors: [
        default: [
          concurrency: processor_concurrency,
          min_demand: @processor_min_demand,
          max_demand: @processor_max_demand
        ]
      ],
      batchers: [
        ch: [
          concurrency: @batcher_concurrency,
          batch_size: @batch_size,
          batch_timeout: @batch_timeout
        ]
      ],
      context: processor_context(backend.id)
    )
  end

  @doc false
  @spec processor_context(pos_integer()) :: map()
  def processor_context(backend_id) do
    mapper_configs =
      Map.new(@event_types, fn event_type ->
        {:ok, compiled, config_id} = MappingConfigStore.get_compiled(event_type)

        {event_type,
         %{
           compiled: compiled,
           mapping_config_id: Ingester.encode_mapping_config_id(config_id)
         }}
      end)

    %{backend_id: backend_id, mapper_configs: mapper_configs}
  end

  @spec process_name(via_tuple :: {:via, module(), {module(), term()}}, base_name :: term()) ::
          {:via, module(), {module(), term()}}
  def process_name({:via, module, {registry, identifier}}, base_name) do
    new_identifier = Utils.append_to_tuple(identifier, base_name)
    {:via, module, {registry, new_identifier}}
  end

  @spec transform(pointer :: LogEventPointer.t(), opts :: keyword()) :: Message.t()
  def transform(%LogEventPointer{} = pointer, opts) do
    # Runs in the same process as the producer itself (Broadway.Topology.ProducerStage
    # calls the producer module's own callbacks, then the transformer, inline, in one
    # process) — so self() here is the producer's pid, and this lookup always finds the
    # ref the producer published at init.
    in_flight_ref = BufferProducer.get_in_flight_ref(self())

    %Message{
      data: pointer,
      acknowledger:
        {__MODULE__, :ack_id, %{backend_id: opts[:backend_id], in_flight_ref: in_flight_ref}}
    }
  end

  @spec handle_message(processor_name :: atom(), message :: Message.t(), context :: map()) ::
          Message.t()
  def handle_message(
        _processor_name,
        %Message{data: %LogEventPointer{event_type: event_type, day_bucket: day_bucket} = pointer} =
          message,
        %{backend_id: backend_id, mapper_configs: mapper_configs}
      )
      when is_event_type(event_type) do
    message =
      message
      |> Message.put_batcher(:ch)
      |> Message.put_batch_key({event_type, day_bucket})

    case IngestEventQueue.lookup_event(pointer.tid, pointer.gen_event_id) do
      %LogEvent{} = event ->
        %{compiled: compiled, mapping_config_id: mapping_config_id} =
          Map.fetch!(mapper_configs, event_type)

        output_context = OutputContext.clickhouse_row_binary(event, mapping_config_id)

        case Mapper.map_result(event.body, compiled, output_context: output_context) do
          {:ok, row} ->
            encoded = %EncodedRow{pointer: pointer, row: row}
            replace_event_with_encoded_row(message, encoded, backend_id, event_type)

          {:error, reason} ->
            Message.failed(message, reason)
        end

      %EncodedRow{} = encoded ->
        replace_event_with_encoded_row(
          message,
          %{encoded | pointer: pointer},
          backend_id,
          event_type
        )

      nil ->
        fail_missing_message(message, backend_id, event_type)
    end
  end

  def handle_message(_processor_name, message, _context) do
    Message.failed(message, :not_found)
  end

  @spec replace_event_with_encoded_row(
          Message.t(),
          EncodedRow.t(),
          pos_integer(),
          TypeDetection.event_type()
        ) :: Message.t()
  defp replace_event_with_encoded_row(
         message,
         %EncodedRow{pointer: pointer} = encoded,
         backend_id,
         event_type
       ) do
    encoded_message = %{message | data: encoded}

    case IngestEventQueue.replace_event(pointer.tid, pointer.gen_event_id, encoded) do
      :ok -> encoded_message
      {:error, :not_found} -> fail_missing_message(message, backend_id, event_type)
    end
  end

  defp fail_missing_message(message, backend_id, event_type) do
    emit_missing_ids_telemetry(backend_id, event_type, 1)
    Message.failed(message, :not_found)
  end

  @spec handle_batch(
          batcher :: atom(),
          messages :: [Message.t()],
          batch_info :: Broadway.BatchInfo.t(),
          context :: map()
        ) :: [Message.t()]
  def handle_batch(_batcher, [], _batch_info, _context), do: []

  def handle_batch(
        :ch,
        messages,
        %{batch_key: {event_type, day_bucket}} = batch_info,
        %{backend_id: backend_id}
      )
      when is_event_type(event_type) do
    emit_batch_telemetry(batch_info, backend_id, event_type, day_bucket)

    backend = Backends.Cache.get_backend(backend_id)

    compress_and_insert(backend, messages, event_type, batch_info, day_bucket)
  end

  @spec ack(ack_ref :: term(), successful :: [Message.t()], failed :: [Message.t()]) :: :ok
  def ack(_ack_ref, successful, failed) do
    decrement_in_flight(successful, failed)

    Enum.each(successful, fn message ->
      pointer = message_pointer(message)
      IngestEventQueue.delete_id(pointer.tid, pointer.gen_event_id)
    end)

    if failed != [] do
      failed
      |> Enum.group_by(fn %{acknowledger: {_, _, ack_data}} -> ack_data.backend_id end)
      |> Enum.each(fn {backend_id, messages} ->
        maybe_requeue_failed(backend_id, messages)
      end)
    end

    :ok
  end

  # Decrements the claiming producer's in-flight counter directly via the atomics ref
  # carried on each message's ack_data (see transform/2) — no message-passing, no
  # cross-process call. By the time ack/3 fires for a message, Broadway has already
  # finished handle_batch/4 for it, so it's no longer sitting in BatcherStage's
  # (effectively unbounded) buffer regardless of what happens to it next.
  @spec decrement_in_flight([Message.t()], [Message.t()]) :: :ok
  defp decrement_in_flight(successful, failed) do
    counts = count_in_flight(successful, %{})
    counts = count_in_flight(failed, counts)

    Enum.each(counts, fn {ref, count} -> :atomics.sub(ref, 1, count) end)
  end

  defp count_in_flight(messages, counts) do
    Enum.reduce(messages, counts, fn %{acknowledger: {_, _, ack_data}}, counts ->
      case Map.get(ack_data, :in_flight_ref) do
        nil -> counts
        ref -> Map.update(counts, ref, 1, &(&1 + 1))
      end
    end)
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
        backend_type: :clickhouse,
        backend_id: backend_id,
        event_type: event_type,
        batch_trigger: batch_info.trigger,
        day_bucket: day_bucket
      }
    )
  end

  @spec compress_and_insert(
          Backend.t(),
          [Message.t()],
          TypeDetection.event_type(),
          Broadway.BatchInfo.t(),
          integer()
        ) :: [Message.t()]
  defp compress_and_insert(backend, messages, event_type, batch_info, day_bucket) do
    OpenTelemetry.Tracer.with_span :clickhouse_pipeline, %{
      attributes: %{
        backend_id: backend.id,
        ingest_batch_size: batch_info.size,
        ingest_batch_trigger: batch_info.trigger,
        event_type: event_type,
        day_bucket: day_bucket
      }
    } do
      {good, rejected, good_count, compressed} = stream_compress(messages, event_type)

      emit_missing_ids_telemetry(rejected, backend, event_type)
      finalize_insert(backend, event_type, compressed, good_count, good, rejected)
    end
  end

  # RowBinary production is complete before batching. This stage owns only the one
  # gzip stream that requires a complete batch.
  @spec stream_compress([Message.t()], TypeDetection.event_type()) ::
          {[Message.t()], [Message.t()], non_neg_integer(), binary()}
  defp stream_compress(messages, event_type) do
    z = :zlib.open()

    try do
      :zlib.deflateInit(z, :default, :deflated, 31, 8, :default)

      {good, rejected, good_count, chunks} =
        Enum.reduce(messages, {[], [], 0, []}, fn message, acc ->
          compress_message(z, event_type, message, acc)
        end)

      final_chunk = :zlib.deflate(z, "", :finish)
      {good, rejected, good_count, IO.iodata_to_binary([Enum.reverse(chunks), final_chunk])}
    after
      :zlib.deflateEnd(z)
      :zlib.close(z)
    end
  end

  @spec compress_message(
          term(),
          TypeDetection.event_type(),
          Message.t(),
          {[Message.t()], [Message.t()], non_neg_integer(), iodata()}
        ) :: {[Message.t()], [Message.t()], non_neg_integer(), iodata()}
  defp compress_message(
         z,
         event_type,
         %{
           data: %EncodedRow{
             pointer: %LogEventPointer{event_type: event_type},
             row: row
           }
         } = message,
         {good, rejected, good_count, chunks}
       ) do
    row_chunk = :zlib.deflate(z, row)
    {[message | good], rejected, good_count + 1, [row_chunk | chunks]}
  end

  defp compress_message(
         _z,
         _event_type,
         message,
         {good, rejected, good_count, chunks}
       ) do
    {good, [message | rejected], good_count, chunks}
  end

  @spec emit_missing_ids_telemetry([Message.t()], Backend.t(), TypeDetection.event_type()) :: :ok
  defp emit_missing_ids_telemetry(rejected, %Backend{} = backend, event_type) do
    count = Enum.count(rejected, &match?(%Message{status: {:failed, :not_found}}, &1))
    emit_missing_ids_telemetry(backend.id, event_type, count)
  end

  @spec emit_missing_ids_telemetry(
          pos_integer(),
          TypeDetection.event_type(),
          non_neg_integer()
        ) :: :ok
  defp emit_missing_ids_telemetry(_backend_id, _event_type, 0), do: :ok

  defp emit_missing_ids_telemetry(backend_id, event_type, count) do
    :telemetry.execute(
      [:logflare, :ingest_event_queue, :missing_ids],
      %{count: count},
      %{backend_type: :clickhouse, backend_id: backend_id, event_type: event_type}
    )
  end

  @spec finalize_insert(
          Backend.t(),
          TypeDetection.event_type(),
          binary(),
          non_neg_integer(),
          [Message.t()],
          [Message.t()]
        ) :: [Message.t()]
  defp finalize_insert(
         _backend,
         _event_type,
         _compressed,
         _good_count,
         [] = _good,
         rejected
       ) do
    # No rows encoded, so skip the empty ClickHouse insert. Rejected messages
    # already carry their mapping failure or missing-event reason.
    rejected
  end

  defp finalize_insert(backend, event_type, compressed, good_count, good, rejected) do
    insert_opts = [async: async_insert?(backend, good_count)]

    case ClickHouseAdaptor.insert_log_events_compressed(
           backend,
           event_type,
           compressed,
           insert_opts
         ) do
      :ok ->
        # `rejected` (rare, typically empty) goes on the left of `++` so the cons
        # cells being rebuilt are its short list; `good` is attached without copying.
        rejected ++ good

      {:error, reason} ->
        record_insert_failure(backend, reason)
        rejected ++ Enum.map(good, &Message.failed(&1, reason))
    end
  end

  @spec async_insert?(Backend.t(), non_neg_integer()) :: boolean()
  defp async_insert?(
         %Backend{
           config: %{use_async_inserts_for_small_batches: true, async_insert_max_rows: max_rows}
         },
         row_count
       )
       when is_pos_integer(max_rows) and is_pos_integer(row_count) and row_count < max_rows,
       do: true

  defp async_insert?(_backend, _row_count), do: false

  @spec record_insert_failure(Backend.t(), term()) :: :ok
  defp record_insert_failure(backend, reason) do
    if Ingester.too_many_parts?(reason) do
      CircuitBreaker.trip(backend)
    else
      CircuitBreaker.record_failure(backend)
    end
  end

  @spec maybe_requeue_failed(backend_id :: pos_integer(), messages :: [Message.t()]) :: :ok
  defp maybe_requeue_failed(backend_id, messages) do
    payloads = Enum.map(messages, & &1.data)

    {retriable, exhausted} =
      Enum.split_with(payloads, &(message_pointer(&1).retries < @max_retries))

    drop_failed(exhausted, backend_id, "exhausted #{@max_retries} retries")

    requeue_or_shed(backend_id, retriable)
  end

  @spec requeue_or_shed(
          backend_id :: pos_integer(),
          retriable :: [EncodedRow.t() | LogEventPointer.t()]
        ) :: :ok
  defp requeue_or_shed(_backend_id, []), do: :ok

  defp requeue_or_shed(backend_id, retriable) do
    case CircuitBreaker.check(backend_id) do
      :ok ->
        requeue_retriable(backend_id, retriable)

      {:error, :circuit_open, _blocked_until} ->
        drop_failed(retriable, backend_id, "circuit breaker open")
    end
  end

  @typep requeue_result :: :requeued | :deduplicated | :lookup_miss | :queue_unavailable

  @spec requeue_retriable(
          backend_id :: pos_integer(),
          retriable :: [EncodedRow.t() | LogEventPointer.t()]
        ) :: :ok
  defp requeue_retriable(backend_id, retriable) do
    Logger.info(
      "Requeuing #{length(retriable)} ClickHouse events for retry",
      backend_id: backend_id
    )

    result_counts = Enum.frequencies_by(retriable, &requeue_payload(backend_id, &1))

    emit_requeue_deduplicated_telemetry(
      backend_id,
      Map.get(result_counts, :deduplicated, 0)
    )

    emit_requeue_lookup_miss_telemetry(backend_id, Map.get(result_counts, :lookup_miss, 0))

    emit_requeue_queue_unavailable_telemetry(
      backend_id,
      Map.get(result_counts, :queue_unavailable, 0)
    )

    :ok
  end

  @spec requeue_payload(pos_integer(), EncodedRow.t() | LogEventPointer.t()) :: requeue_result()
  defp requeue_payload(backend_id, %EncodedRow{pointer: pointer} = encoded) do
    transfer_retry_payload(backend_id, pointer, encoded)
  end

  defp requeue_payload(backend_id, %LogEventPointer{} = pointer) do
    case IngestEventQueue.lookup_event(pointer.tid, pointer.gen_event_id) do
      nil -> :lookup_miss
      event_or_encoded_row -> transfer_retry_payload(backend_id, pointer, event_or_encoded_row)
    end
  end

  @spec transfer_retry_payload(pos_integer(), LogEventPointer.t(), EncodedRow.t() | LogEvent.t()) ::
          requeue_result()
  defp transfer_retry_payload(backend_id, pointer, payload) do
    pointer = %{pointer | retries: pointer.retries + 1}

    {:consolidated, backend_id}
    |> IngestEventQueue.requeue_payload(pointer, fn new_pointer ->
      retry_payload_with_pointer(payload, new_pointer)
    end)
    |> requeue_transfer_result()
  end

  defp retry_payload_with_pointer(%EncodedRow{} = encoded, pointer),
    do: %{encoded | pointer: pointer}

  defp retry_payload_with_pointer(%LogEvent{} = event, pointer),
    do: %{event | retries: pointer.retries}

  defp requeue_transfer_result({:ok, _pointer}), do: :requeued
  defp requeue_transfer_result({:error, :already_exists}), do: :deduplicated
  defp requeue_transfer_result({:error, :not_initialized}), do: :queue_unavailable

  @spec emit_requeue_deduplicated_telemetry(pos_integer(), non_neg_integer()) :: :ok
  defp emit_requeue_deduplicated_telemetry(_backend_id, 0), do: :ok

  defp emit_requeue_deduplicated_telemetry(backend_id, deduplicated_count) do
    Logger.warning(
      "Deduplicated #{deduplicated_count} ClickHouse event(s) during retry requeue: a newer same-ID pointer remains queued",
      backend_id: backend_id
    )

    :telemetry.execute(
      [:logflare, :ingest_event_queue, :requeue_deduplicated],
      %{count: deduplicated_count},
      %{backend_type: :clickhouse, backend_id: backend_id}
    )
  end

  # A lookup miss means GenerationJanitor dropped the backing generation before the
  # retry could resolve its event. Bounded and rare in practice, but worth
  # surfacing since it's otherwise invisible.
  @spec emit_requeue_lookup_miss_telemetry(pos_integer(), non_neg_integer()) :: :ok
  defp emit_requeue_lookup_miss_telemetry(_backend_id, 0), do: :ok

  defp emit_requeue_lookup_miss_telemetry(backend_id, missing_count) do
    Logger.warning(
      "Dropped #{missing_count} ClickHouse event(s) during retry requeue: pointer's generation was already gone by lookup time",
      backend_id: backend_id
    )

    :telemetry.execute(
      [:logflare, :ingest_event_queue, :requeue_lookup_miss],
      %{count: missing_count},
      %{backend_id: backend_id}
    )
  end

  @spec emit_requeue_queue_unavailable_telemetry(pos_integer(), non_neg_integer()) :: :ok
  defp emit_requeue_queue_unavailable_telemetry(_backend_id, 0), do: :ok

  defp emit_requeue_queue_unavailable_telemetry(backend_id, dropped_count) do
    Logger.warning(
      "Dropped #{dropped_count} ClickHouse event(s) during retry requeue: no queue remained available",
      backend_id: backend_id
    )

    :telemetry.execute(
      [:logflare, :ingest_event_queue, :not_initialized, :dropped],
      %{count: dropped_count},
      %{backend_type: :clickhouse, backend_id: backend_id}
    )
  end

  @spec drop_failed(
          payloads :: [EncodedRow.t() | LogEventPointer.t()],
          backend_id :: pos_integer(),
          reason :: String.t()
        ) :: :ok
  defp drop_failed([], _backend_id, _reason), do: :ok

  defp drop_failed(payloads, backend_id, reason) do
    Logger.warning(
      "Dropping #{length(payloads)} ClickHouse events: #{reason}",
      backend_id: backend_id
    )

    Enum.each(payloads, fn payload ->
      pointer = message_pointer(payload)
      IngestEventQueue.delete_id(pointer.tid, pointer.gen_event_id)
    end)
  end

  @spec message_pointer(Message.t() | EncodedRow.t() | LogEventPointer.t()) :: LogEventPointer.t()
  defp message_pointer(%Message{data: data}), do: message_pointer(data)
  defp message_pointer(%EncodedRow{pointer: pointer}), do: pointer
  defp message_pointer(%LogEventPointer{} = pointer), do: pointer
end
