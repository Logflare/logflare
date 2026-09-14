defmodule Logflare.Backends.Spool.ConsumerPipeline do
  @moduledoc false

  use Broadway

  require Logger

  alias Broadway.Message
  alias Logflare.Backends
  alias Logflare.Backends.Spool.ConsumerPipeline.QueueProducer
  alias Logflare.Backends.Spool.MemoryMonitor
  alias Logflare.Backends.Spool.Queue
  alias Logflare.Backends.Spool.Storage
  alias Logflare.Sources

  @behaviour Broadway.Acknowledger

  # Flat byte budget, not derived from batch_size/concurrency — max_in_flight
  # is a byte budget (see QueueProducer), and a single segment can already be
  # tens of KB, so a formula scaled off segment *count* (e.g. batch_size)
  # undershoots by orders of magnitude. 2GB is a temporary, deliberately
  # generous placeholder pending a properly-tuned default.
  @default_max_in_flight_bytes 2 * 1024 * 1024 * 1024

  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(args) do
    {name, _args} = Keyword.pop!(args, :name)

    spool_config = Application.get_env(:logflare, :spool, [])
    bucket = Keyword.fetch!(spool_config, :bucket)

    concurrency =
      Keyword.get(spool_config, :consumer_concurrency, max(System.schedulers_online(), 4))

    # Now counts segments, not events (each Broadway item is one segment —
    # parsing happens in handle_message/3), so the default is far smaller
    # than the old event-based 500 — needs tuning against real segment-size
    # distribution in production.
    batch_size = Keyword.get(spool_config, :consumer_batch_size, 20)
    queue_name = Keyword.fetch!(spool_config, :queue_name)
    provider = Keyword.get(spool_config, :provider, :aws)
    storage_mod = Keyword.get(spool_config, :storage_mod, default_storage_mod(provider))
    queue_mod = Keyword.get(spool_config, :queue_mod, default_queue_mod(provider))
    queue_url = resolve_queue_url!(queue_name, queue_mod)

    max_in_flight =
      Keyword.get(spool_config, :consumer_max_in_flight_bytes, @default_max_in_flight_bytes)

    Broadway.start_link(__MODULE__,
      name: name,
      producer: [
        module:
          {QueueProducer,
           [
             queue_url: queue_url,
             bucket: bucket,
             storage_mod: storage_mod,
             queue_mod: queue_mod,
             max_in_flight: max_in_flight
           ]},
        transformer: {__MODULE__, :transform, []}
      ],
      processors: [
        default: [concurrency: concurrency, min_demand: 2, max_demand: 10]
      ],
      batchers: [
        default: [
          batch_size: batch_size,
          batch_timeout: 200,
          concurrency: concurrency
        ]
      ]
    )
  end

  @spec transform(map(), keyword()) :: Message.t()
  def transform(%{segment: segment} = unparsed, _opts) do
    in_flight_ref = QueueProducer.get_in_flight_ref()

    %Message{
      data: unparsed,
      acknowledger:
        {__MODULE__, :noop, %{in_flight_ref: in_flight_ref, bytes: byte_size(segment)}}
    }
  end

  # Queue acking (SQS/PubSub) is managed by the producer — individual message
  # ack is a no-op there. This still has to decrement the producer's
  # max_in_flight counter, the other half of QueueProducer's emit-side cap.
  @impl Broadway.Acknowledger
  def ack(_ack_ref, successful, failed) do
    decrement_in_flight(successful ++ failed)

    if failed != [] do
      :telemetry.execute(
        [:logflare, :backends, :spool, :consumer, :messages_failed],
        %{count: length(failed)},
        %{}
      )

      Logger.error("spool_consumer: #{length(failed)} messages failed during processing")
    end

    :ok
  end

  @spec decrement_in_flight([Message.t()]) :: :ok
  defp decrement_in_flight(messages) do
    messages
    |> Enum.group_by(&in_flight_ref_of/1, &bytes_of/1)
    |> Enum.each(fn
      {nil, _bytes} -> :ok
      {ref, bytes} -> :atomics.sub(ref, 1, Enum.sum(bytes))
    end)
  end

  # Tolerates the shapes hand-built test messages use (acknowledger: nil, or
  # a {mod, ref, nil} placeholder) as well as the real transform/2 output.
  defp in_flight_ref_of(%{acknowledger: {_, _, %{} = ack_data}}),
    do: Map.get(ack_data, :in_flight_ref)

  defp in_flight_ref_of(_), do: nil

  defp bytes_of(%{acknowledger: {_, _, %{} = ack_data}}), do: Map.get(ack_data, :bytes, 0)
  defp bytes_of(_), do: 0

  # Parses one segment (deferred by QueueProducer so this runs with real
  # processor concurrency instead of serialized in the producer — see
  # QueueProducer's "Spool file format" moduledoc section). A segment that
  # fails to parse (passes CRC but isn't valid content) fails just this one
  # message; Broadway routes it straight to ack/3's `failed` list without
  # ever reaching handle_batch/4.
  @impl Broadway
  def handle_message(
        _processor,
        %Message{data: %{segment: segment, format: format}} = message,
        _context
      ) do
    {duration, result} = :timer.tc(fn -> parse_segment(segment, format) end)

    case result do
      {:ok, records} ->
        :telemetry.execute(
          [:logflare, :backends, :spool, :consumer, :parse],
          %{duration: duration, segment_count: 1, event_count: length(records)},
          %{}
        )

        Enum.each(records, &maybe_register_source/1)
        %{message | data: records}

      {:error, reason} ->
        Logger.error("spool_consumer: failed to parse segment, discarding: #{inspect(reason)}")
        Message.failed(message, reason)
    end
  end

  defp parse_segment(content, format) do
    {:ok, do_parse_segment(content, format)}
  rescue
    e -> {:error, e}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp do_parse_segment(content, :etf), do: :erlang.binary_to_term(content)

  defp do_parse_segment(content, :ndjson) do
    content
    |> String.split("\n", trim: true)
    |> Enum.flat_map(&decode_json_line/1)
  end

  defp decode_json_line(line) do
    case Jason.decode(line) do
      {:ok, map} -> [map]
      {:error, _} -> []
    end
  end

  # Lets MemoryMonitor know this source is currently flowing through the
  # spool consumer, so its refresh cycle checks its destination ingest buffer
  # for backlog. register_source/1's cast handler is idempotent (MapSet.put),
  # so no dedup bookkeeping is needed here — unlike when this lived in
  # QueueProducer, one process's worth of state can't be shared across
  # concurrent processors anyway.
  defp maybe_register_source(record) do
    case record_source_id(record) do
      nil -> :ok
      source_id -> MemoryMonitor.register_source(source_id)
    end
  end

  @impl Broadway
  def handle_batch(_batcher, messages, batch_info, _context) do
    batch_size = Map.get(batch_info, :size)
    batch_trigger = Map.get(batch_info, :trigger)

    :telemetry.execute(
      [:logflare, :backends, :pipeline, :handle_batch],
      %{batch_size: batch_size, batch_trigger: batch_trigger},
      %{backend_type: :spool_consumer, batch_trigger: batch_trigger}
    )

    failed_source_ids =
      messages
      |> Enum.flat_map(fn message -> Enum.map(message.data, &{message, &1}) end)
      |> Enum.group_by(fn {_message, record} -> record_source_id(record) end, fn {_message,
                                                                                  record} ->
        record
      end)
      |> Enum.flat_map(fn
        {nil, records} ->
          emit_skipped_telemetry(:missing_source_id, length(records))
          Logger.debug("spool_consumer: #{length(records)} events missing source_id, skipping")
          []

        {source_id, records} ->
          dispatch_group(source_id, records)
      end)
      |> MapSet.new()

    fail_dispatched(messages, failed_source_ids)
  end

  defp fail_dispatched(messages, failed_source_ids) do
    if Enum.empty?(failed_source_ids) do
      messages
    else
      Enum.map(messages, &fail_message(&1, failed_source_ids))
    end
  end

  # A message is one segment's worth of records — a segment is always a
  # single original ingest request's chunk, so in practice every record in
  # it shares one source_id, but this checks all of them rather than
  # assuming that.
  defp fail_message(message, failed_source_ids) do
    if Enum.any?(message.data, &MapSet.member?(failed_source_ids, record_source_id(&1))) do
      Message.failed(message, :dispatch_error)
    else
      message
    end
  end

  defp dispatch_group(source_id, lines) do
    case Sources.Cache.get_by(id: source_id) do
      nil ->
        emit_skipped_telemetry(:unknown_source_id, length(lines))

        Logger.debug(
          "spool_consumer: unknown source_id=#{source_id}, skipping #{length(lines)} events"
        )

        []

      source ->
        {:ok, _} = Backends.dispatch_from_spool(lines, source)
        []
    end
  rescue
    exception ->
      emit_skipped_telemetry(:dispatch_error, length(lines))

      Logger.error(
        "spool_consumer: dispatch failed for source_id=#{source_id}, " <>
          "failing #{length(lines)} events: " <>
          Exception.format(:error, exception, __STACKTRACE__)
      )

      [source_id]
  end

  defp emit_skipped_telemetry(reason, count) do
    :telemetry.execute(
      [:logflare, :backends, :spool, :consumer, :skipped],
      %{count: count},
      %{reason: reason}
    )
  end

  defp resolve_queue_url!(queue_name, queue_mod) do
    case queue_mod.resolve(queue_name) do
      {:ok, ref} ->
        ref

      {:error, reason} ->
        raise "spool_consumer: failed to resolve queue ref for #{queue_name}: #{inspect(reason)}"
    end
  end

  defp record_source_id(%{source_id: id}), do: id
  defp record_source_id(%{"source_id" => id}), do: id
  defp record_source_id(_), do: nil

  defp default_storage_mod(:gcp), do: Storage.GCS
  defp default_storage_mod(_), do: Storage.S3

  defp default_queue_mod(:gcp), do: Queue.PubSub
  defp default_queue_mod(_), do: Queue.SQS
end
