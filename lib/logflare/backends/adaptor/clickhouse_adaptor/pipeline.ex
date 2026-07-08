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

  Uses ID-passing: the producer emits `{id, tid, size}` tuples and events
  remain in ETS as `:processing` throughout. Each batcher fetches events
  from ETS and streams them through zlib deflate to build a gzip-compressed
  RowBinary payload without holding the full batch in process memory.
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
  alias Logflare.LogEvent
  alias Logflare.LogEvent.TypeDetection
  alias Logflare.Mapper
  alias Logflare.Utils

  @producer_concurrency 1
  @processor_concurrency 6
  @fresh_batch_size 60_000
  @fresh_batch_timeout 5_000
  @fresh_batcher_concurrency 4
  @stale_batch_size 60_000
  @stale_batch_timeout 12_000
  @stale_batcher_concurrency 2
  @max_retries 2

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
      spawn_opt: [fullsweep_after: 100],
      producer: [
        module: {BufferProducer, [backend_id: backend.id, consolidated: true, id_passing: true]},
        transformer: {__MODULE__, :transform, [backend_id: backend.id]},
        concurrency: @producer_concurrency
      ],
      processors: [
        default: [concurrency: @processor_concurrency, min_demand: 1, max_demand: 100]
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

  @spec transform(event :: {term(), :ets.tid(), non_neg_integer()}, opts :: keyword()) ::
          Message.t()
  def transform({id, tid, _size}, opts) do
    %Message{
      data: {id, tid},
      acknowledger: {__MODULE__, :ack_id, %{backend_id: opts[:backend_id]}}
    }
  end

  @spec handle_message(processor_name :: atom(), message :: Message.t(), context :: map()) ::
          Message.t()
  def handle_message(_processor_name, %Message{data: {id, tid}} = message, _context) do
    case IngestEventQueue.lookup_id(tid, id) do
      {^id, :processing, %LogEvent{event_type: event_type, day_bucket: day_bucket} = event, _}
      when is_event_type(event_type) ->
        message
        |> Message.put_data({id, tid, event_type, day_bucket})
        |> Message.put_batcher(ingest_freshness_to_batcher(event.ingest_freshness))
        |> Message.put_batch_key({event_type, day_bucket})

      _ ->
        Message.failed(message, :not_found)
    end
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
    Enum.each(successful, fn %{data: {id, tid, _event_type, _day_bucket}} ->
      IngestEventQueue.delete_id(tid, id)
    end)

    if failed != [] do
      failed
      |> Enum.group_by(fn %{acknowledger: {_, _, ack_data}} -> ack_data end)
      |> Enum.each(fn {%{backend_id: backend_id}, messages} ->
        maybe_requeue_failed(backend_id, messages)
      end)
    end

    :ok
  rescue
    e ->
      Logger.warning(
        "ClickHouse pipeline ack error (ETS table may have been recycled): #{inspect(e)}"
      )

      :ok
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
        z = :zlib.open()

        try do
          :zlib.deflateInit(z, :default, :deflated, 31, 8, :default)

          {good, bad, chunks} =
            Enum.reduce(messages, {[], [], []}, fn
              %{data: {id, tid, ^event_type, _day_bucket}} = msg, {g, b, acc} ->
                case IngestEventQueue.lookup_id(tid, id) do
                  {^id, :processing, %LogEvent{} = event, _} ->
                    mapped_body =
                      event.body
                      |> Mapper.map(compiled)
                      |> Map.put("mapping_config_id", config_id)
                      |> maybe_compute_duration(event_type)
                      |> resolve_severity_number(event_type)

                    row_chunk =
                      :zlib.deflate(
                        z,
                        Ingester.encode_row(%{event | body: mapped_body}, event_type)
                      )

                    {[msg | g], b, [acc, row_chunk]}

                  _ ->
                    {g, [Message.failed(msg, :not_found) | b], acc}
                end

              msg, {g, b, acc} ->
                {g, [Message.failed(msg, :not_found) | b], acc}
            end)

          miss_count = length(bad)

          if miss_count > 0 do
            Logger.warning(
              "ClickHouse pipeline: #{miss_count} ETS misses in handle_batch, events lost",
              backend_id: backend.id,
              event_type: event_type
            )
          end

          final_chunk = :zlib.deflate(z, "", :finish)
          compressed = IO.iodata_to_binary([chunks, final_chunk])

          case ClickHouseAdaptor.insert_log_events_compressed(
                 backend,
                 event_type,
                 compressed,
                 async: batcher_async?(batcher)
               ) do
            :ok ->
              Enum.reverse(good) ++ Enum.reverse(bad)

            {:error, reason} ->
              CircuitBreaker.record_failure(backend)
              Enum.map(Enum.reverse(good) ++ Enum.reverse(bad), &Message.failed(&1, reason))
          end
        after
          :zlib.deflateEnd(z)
          :zlib.close(z)
        end
      else
        {:error, reason} ->
          Enum.map(messages, &Message.failed(&1, reason))
      end
    end
  end

  @spec maybe_requeue_failed(backend_id :: pos_integer(), messages :: [Message.t()]) :: :ok
  defp maybe_requeue_failed(backend_id, messages) do
    {retriable, exhausted} = Enum.split_with(messages, &retriable?/1)

    drop_failed(exhausted, backend_id, "exhausted #{@max_retries} retries")
    requeue_or_shed(backend_id, retriable)
  end

  @spec retriable?(Message.t()) :: boolean()
  defp retriable?(%{data: {id, tid, _event_type, _day_bucket}}) do
    case IngestEventQueue.lookup_id(tid, id) do
      {^id, _, %LogEvent{retries: retries}, _} -> (retries || 0) < @max_retries
      _ -> false
    end
  end

  defp retriable?(_message), do: false

  @spec requeue_or_shed(backend_id :: pos_integer(), retriable :: [Message.t()]) :: :ok
  defp requeue_or_shed(_backend_id, []), do: :ok

  defp requeue_or_shed(backend_id, retriable) do
    case CircuitBreaker.check(backend_id) do
      :ok ->
        requeue_retriable(backend_id, retriable)

      {:error, :circuit_open, _blocked_until} ->
        drop_failed(retriable, backend_id, "circuit breaker open")
    end
  end

  @spec requeue_retriable(backend_id :: pos_integer(), retriable :: [Message.t()]) :: :ok
  defp requeue_retriable(_backend_id, []), do: :ok

  defp requeue_retriable(backend_id, retriable) do
    key = {:consolidated, backend_id}
    events_to_requeue = Enum.flat_map(retriable, &bump_retries/1)

    if events_to_requeue != [] do
      Logger.info(
        "Requeuing #{length(events_to_requeue)} ClickHouse events for retry",
        backend_id: backend_id
      )

      IngestEventQueue.delete_batch(key, events_to_requeue)
      IngestEventQueue.add_to_table(key, events_to_requeue)
    end

    :ok
  end

  @spec bump_retries(Message.t()) :: [LogEvent.t()]
  defp bump_retries(%{data: {id, tid, _event_type, _day_bucket}}) do
    case IngestEventQueue.lookup_id(tid, id) do
      {^id, _, %LogEvent{} = event, _} -> [%LogEvent{event | retries: (event.retries || 0) + 1}]
      _ -> []
    end
  end

  defp bump_retries(_message), do: []

  @spec drop_failed(
          messages :: [Message.t()],
          backend_id :: pos_integer(),
          reason :: String.t()
        ) :: :ok
  defp drop_failed([], _backend_id, _reason), do: :ok

  defp drop_failed(messages, backend_id, reason) do
    Logger.warning(
      "Dropping #{length(messages)} ClickHouse events: #{reason}",
      backend_id: backend_id
    )

    Enum.each(messages, fn %{data: data} ->
      case data do
        {id, tid, _event_type, _day_bucket} -> IngestEventQueue.delete_id(tid, id)
        {id, tid} -> IngestEventQueue.delete_id(tid, id)
        _ -> :ok
      end
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
