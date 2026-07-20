defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.Pipeline do
  @moduledoc """
  Broadway pipeline for the ClickHouse adaptor.

  This pipeline is responsible for taking log events from the
  consolidated queue and inserting them into the backend's type-specific
  ingest tables (otel_logs, otel_metrics, otel_traces).

  Events are routed to one of two batchers (`:ch_fresh` or `:ch_stale`)
  based on `LogEvent.ingest_freshness`, and batched by a composite key of
  `{event_type, day_bucket}` so each insert targets a single ClickHouse
  partition. Multiple sources are processed together in a single pipeline.

  Uses ID-passing: the producer emits `LogEventPointer`s (id + routing metadata)
  while the full events live in a separate generation store (see
  `Logflare.Backends.IngestEventQueue`). Each batcher resolves the pointer to its event
  lazily and streams it through zlib deflate to build a gzip-compressed RowBinary
  payload without holding the full batch in process memory.
  """

  @behaviour Broadway.Acknowledger

  import Logflare.Utils.Guards, only: [is_event_type: 1]

  require Logger
  require OpenTelemetry.Tracer

  alias Broadway.Message
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.CircuitBreaker
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.Ingester
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.MappingConfigStore
  alias Logflare.Backends.Backend
  alias Logflare.Backends.BufferProducer
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.IngestEventQueue.LogEventPointer
  alias Logflare.LogEvent
  alias Logflare.LogEvent.TypeDetection
  alias Logflare.Mapper
  alias Logflare.Utils

  @producer_concurrency 1
  @processor_concurrency 6
  @processor_min_demand 100
  @processor_max_demand 1_000
  @fresh_batch_size 60_000
  @fresh_batch_timeout 5_000
  @fresh_batcher_concurrency 64
  @stale_batch_size 60_000
  @stale_batch_timeout 12_000
  @stale_batcher_concurrency 16
  @max_retries 1
  # One full batch per fresh/stale batcher lane, used as a generous safety valve rather
  # than a fine-grained flow-control knob — see BufferProducer.capped_fetch_amount/2.
  # It should only cap genuinely runaway backlog during healthy operation.
  @max_in_flight 2 * @fresh_batch_size * @fresh_batcher_concurrency +
                   @stale_batch_size * @stale_batcher_concurrency

  @doc false
  @spec max_retries() :: non_neg_integer()
  def max_retries, do: @max_retries

  @doc false
  @spec max_batch_size() :: pos_integer()
  def max_batch_size, do: @fresh_batch_size

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
             seed_batch_size: @fresh_batch_size
           ]},
        transformer: {__MODULE__, :transform, [backend_id: backend.id]},
        concurrency: @producer_concurrency
      ],
      processors: [
        default: [
          concurrency: @processor_concurrency,
          min_demand: @processor_min_demand,
          max_demand: @processor_max_demand
        ]
      ],
      batchers: [
        ch_fresh: [
          concurrency: @fresh_batcher_concurrency,
          batch_size: @fresh_batch_size,
          batch_timeout: @fresh_batch_timeout
        ],
        ch_stale: [
          concurrency: @stale_batcher_concurrency,
          batch_size: @stale_batch_size,
          batch_timeout: @stale_batch_timeout
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

  @spec transform(pointer :: LogEventPointer.t(), opts :: keyword()) :: Message.t()
  def transform(%LogEventPointer{} = pointer, opts) do
    # Runs in the same process as the producer itself (Broadway.Topology.ProducerStage
    # calls the producer module's own callbacks, then the transformer, inline, in one
    # process) — so self() here is the producer's pid, and this lookup always finds the
    # ref the producer published at init.
    in_flight_ref = :persistent_term.get({BufferProducer, :in_flight_ref, self()}, nil)

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
        %Message{data: %LogEventPointer{event_type: event_type, ingest_freshness: freshness}} =
          message,
        _context
      )
      when is_event_type(event_type) and freshness in [:fresh, :stale] do
    message
    |> Message.put_batcher(ingest_freshness_to_batcher(freshness))
    |> Message.put_batch_key({event_type, message.data.day_bucket})
  end

  def handle_message(_processor_name, message, _context) do
    Message.failed(message, :not_found)
  end

  @spec ingest_freshness_to_batcher(:fresh | :stale) :: :ch_fresh | :ch_stale
  defp ingest_freshness_to_batcher(:fresh), do: :ch_fresh
  defp ingest_freshness_to_batcher(:stale), do: :ch_stale

  @spec batcher_async?(:ch_fresh | :ch_stale) :: boolean()
  defp batcher_async?(:ch_stale), do: true
  defp batcher_async?(:ch_fresh), do: false

  @spec handle_batch(
          batcher :: atom(),
          messages :: [Message.t()],
          batch_info :: Broadway.BatchInfo.t(),
          context :: map()
        ) :: [Message.t()]
  def handle_batch(_batcher, [], _batch_info, _context), do: []

  def handle_batch(
        batcher,
        messages,
        %{batch_key: {event_type, day_bucket}} = batch_info,
        %{backend_id: backend_id}
      )
      when batcher in [:ch_fresh, :ch_stale] and is_event_type(event_type) do
    emit_batch_telemetry(batch_info, backend_id, event_type, batcher, day_bucket)

    backend = Backends.Cache.get_backend(backend_id)

    encode_and_insert(backend, messages, event_type, batcher, batch_info, day_bucket)
  end

  @spec ack(ack_ref :: term(), successful :: [Message.t()], failed :: [Message.t()]) :: :ok
  def ack(_ack_ref, successful, failed) do
    decrement_in_flight(successful, failed)

    Enum.each(successful, fn %{data: %LogEventPointer{} = pointer} ->
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
          atom(),
          integer()
        ) :: :ok
  defp emit_batch_telemetry(batch_info, backend_id, event_type, batcher, day_bucket) do
    :telemetry.execute(
      [:logflare, :backends, :pipeline, :handle_batch],
      %{batch_size: batch_info.size, batch_trigger: batch_info.trigger},
      %{
        backend_type: :clickhouse,
        backend_id: backend_id,
        event_type: event_type,
        batcher: batcher,
        day_bucket: day_bucket
      }
    )
  end

  @spec encode_and_insert(
          Backend.t(),
          [Message.t()],
          TypeDetection.event_type(),
          atom(),
          Broadway.BatchInfo.t(),
          integer()
        ) :: [Message.t()]
  defp encode_and_insert(backend, messages, event_type, batcher, batch_info, day_bucket) do
    OpenTelemetry.Tracer.with_span :clickhouse_pipeline, %{
      attributes: %{
        backend_id: backend.id,
        ingest_batch_size: batch_info.size,
        ingest_batch_trigger: batch_info.trigger,
        event_type: event_type,
        batcher: batcher,
        day_bucket: day_bucket
      }
    } do
      with {:ok, compiled, config_id} <- MappingConfigStore.get_compiled(event_type) do
        {good, bad, compressed} = stream_compress(messages, event_type, compiled, config_id)
        emit_missing_ids_telemetry(bad, backend, event_type)
        finalize_insert(backend, event_type, batcher, compressed, good, bad)
      else
        {:error, reason} -> Enum.map(messages, &Message.failed(&1, reason))
      end
    end
  end

  # Streams each message's event through the mapper + RowBinary encoder directly into
  # a gzip zlib stream, so the full batch is never materialized as a flat binary.
  # Returns the messages that encoded successfully, the ones missing from ETS, and the
  # finished compressed payload.
  @spec stream_compress([Message.t()], TypeDetection.event_type(), reference(), String.t()) ::
          {[Message.t()], [Message.t()], binary()}
  defp stream_compress(messages, event_type, compiled, config_id) do
    z = :zlib.open()

    try do
      :zlib.deflateInit(z, :default, :deflated, 31, 8, :default)

      # good/bad end up reversed relative to `messages`; that's fine — Broadway
      # partitions and re-reverses handle_batch/4's return by status internally,
      # and neither the acknowledger nor ClickHouse cares about row order.
      # This value is constant for the batch. Keep it out of every mapped body and
      # pass its already-encoded form directly to the row encoder.
      mapping_config_id = Ingester.encode_mapping_config_id(config_id)

      {good, bad, chunks} =
        Enum.reduce(messages, {[], [], []}, fn message, acc ->
          encode_message(z, event_type, compiled, mapping_config_id, message, acc)
        end)

      final_chunk = :zlib.deflate(z, "", :finish)
      {good, bad, IO.iodata_to_binary([Enum.reverse(chunks), final_chunk])}
    after
      :zlib.deflateEnd(z)
      :zlib.close(z)
    end
  end

  @spec encode_message(
          term(),
          TypeDetection.event_type(),
          reference(),
          iodata(),
          Message.t(),
          {[Message.t()], [Message.t()], iodata()}
        ) :: {[Message.t()], [Message.t()], iodata()}
  defp encode_message(
         z,
         event_type,
         compiled,
         mapping_config_id,
         %{data: %LogEventPointer{event_type: msg_event_type} = pointer} = message,
         {good, bad, chunks}
       )
       when msg_event_type == event_type do
    case IngestEventQueue.lookup_event(pointer.tid, pointer.gen_event_id) do
      %LogEvent{} = event ->
        mapped_body =
          event.body
          |> Mapper.map(compiled)
          |> maybe_compute_duration(event_type)
          |> resolve_severity_number(event_type)

        row_chunk =
          :zlib.deflate(
            z,
            Ingester.encode_row(%{event | body: mapped_body}, event_type, mapping_config_id)
          )

        {[message | good], bad, [row_chunk | chunks]}

      nil ->
        {good, [message | bad], chunks}
    end
  end

  defp encode_message(
         _z,
         _event_type,
         _compiled,
         _mapping_config_id,
         message,
         {good, bad, chunks}
       ) do
    {good, [message | bad], chunks}
  end

  @spec emit_missing_ids_telemetry([Message.t()], Backend.t(), TypeDetection.event_type()) :: :ok
  defp emit_missing_ids_telemetry([], _backend, _event_type), do: :ok

  defp emit_missing_ids_telemetry(bad, backend, event_type) do
    :telemetry.execute(
      [:logflare, :ingest_event_queue, :missing_ids],
      %{count: length(bad)},
      %{backend_id: backend.id, event_type: event_type}
    )
  end

  @spec finalize_insert(
          Backend.t(),
          TypeDetection.event_type(),
          atom(),
          binary(),
          [Message.t()],
          [Message.t()]
        ) :: [Message.t()]
  defp finalize_insert(_backend, _event_type, _batcher, _compressed, [] = _good, bad) do
    # No rows encoded (every event was missing from ETS by batch time), so the
    # compressed payload carries zero RowBinary rows. Skip the empty ClickHouse
    # insert and fail the missing messages, mirroring Ingester.insert/5's empty guard.
    Enum.map(bad, &Message.failed(&1, :not_found))
  end

  defp finalize_insert(backend, event_type, batcher, compressed, good, bad) do
    case ClickHouseAdaptor.insert_log_events_compressed(
           backend,
           event_type,
           compressed,
           async: batcher_async?(batcher)
         ) do
      :ok ->
        # `bad` (rare, typically empty) goes on the left of `++` so the cons cells
        # being rebuilt are its short list; `good` (up to the full batch size) is
        # attached as-is on the right with no copying.
        Enum.map(bad, &Message.failed(&1, :not_found)) ++ good

      {:error, reason} ->
        CircuitBreaker.record_failure(backend)
        Enum.map(bad, &Message.failed(&1, reason)) ++ Enum.map(good, &Message.failed(&1, reason))
    end
  end

  @spec maybe_requeue_failed(backend_id :: pos_integer(), messages :: [Message.t()]) :: :ok
  defp maybe_requeue_failed(backend_id, messages) do
    {retriable, exhausted} =
      messages
      |> Enum.map(fn %{data: %LogEventPointer{} = pointer} -> pointer end)
      |> Enum.split_with(&(&1.retries < @max_retries))

    drop_failed(exhausted, backend_id, "exhausted #{@max_retries} retries")

    requeue_or_shed(backend_id, retriable)
  end

  @spec requeue_or_shed(backend_id :: pos_integer(), retriable :: [LogEventPointer.t()]) :: :ok
  defp requeue_or_shed(_backend_id, []), do: :ok

  defp requeue_or_shed(backend_id, retriable) do
    case CircuitBreaker.check(backend_id) do
      :ok ->
        requeue_retriable(backend_id, retriable)

      {:error, :circuit_open, _blocked_until} ->
        drop_failed(retriable, backend_id, "circuit breaker open")
    end
  end

  @spec requeue_retriable(backend_id :: pos_integer(), retriable :: [LogEventPointer.t()]) :: :ok
  defp requeue_retriable(backend_id, retriable) do
    Logger.info(
      "Requeuing #{length(retriable)} ClickHouse events for retry",
      backend_id: backend_id
    )

    events =
      for pointer <- retriable,
          event = IngestEventQueue.lookup_event(pointer.tid, pointer.gen_event_id),
          not is_nil(event) do
        IngestEventQueue.delete_id(pointer.tid, pointer.gen_event_id)
        %{event | retries: pointer.retries + 1}
      end

    emit_requeue_lookup_miss_telemetry(backend_id, length(retriable) - length(events))

    if events != [], do: IngestEventQueue.add_to_table({:consolidated, backend_id}, events)

    :ok
  end

  # A miss here means the pointer's generation was already dropped by
  # GenerationJanitor before the retry could look it up — the event is silently
  # lost rather than requeued. Bounded and rare in practice, but worth surfacing
  # since it's otherwise invisible.
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

  @spec drop_failed(
          pointers :: [LogEventPointer.t()],
          backend_id :: pos_integer(),
          reason :: String.t()
        ) :: :ok
  defp drop_failed([], _backend_id, _reason), do: :ok

  defp drop_failed(pointers, backend_id, reason) do
    Logger.warning(
      "Dropping #{length(pointers)} ClickHouse events: #{reason}",
      backend_id: backend_id
    )

    Enum.each(pointers, fn pointer ->
      IngestEventQueue.delete_id(pointer.tid, pointer.gen_event_id)
    end)
  end

  @spec maybe_compute_duration(map(), TypeDetection.event_type()) :: map()
  defp maybe_compute_duration(
         %{"start_time" => start_time, "end_time" => end_time, "duration" => 0} = body,
         :trace
       )
       when is_integer(start_time) and is_integer(end_time) and end_time > start_time do
    %{body | "duration" => end_time - start_time}
  end

  defp maybe_compute_duration(body, _event_type), do: body

  @spec resolve_severity_number(map(), TypeDetection.event_type()) :: map()
  defp resolve_severity_number(
         %{"severity_number_alt" => alt} = body,
         :log
       )
       when is_integer(alt) and alt > 0 do
    %{body | "severity_number" => alt}
  end

  defp resolve_severity_number(body, _event_type), do: body
end
