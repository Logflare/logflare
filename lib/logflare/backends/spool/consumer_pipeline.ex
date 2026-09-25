defmodule Logflare.Backends.Spool.ConsumerPipeline do
  @moduledoc false

  use Broadway

  require Logger

  alias Broadway.Message
  alias Logflare.Backends
  alias Logflare.Backends.Spool.ConsumerPipeline.QueueProducer
  alias Logflare.Backends.Spool.MemoryMonitor
  alias Logflare.Backends.Spool.ProviderConfig
  alias Logflare.Backends.Spool.SpoolAck
  alias Logflare.Sources

  @behaviour Broadway.Acknowledger

  # Byte budget for QueueProducer's max_in_flight — a temporary, generous
  # placeholder pending a properly-tuned default.
  @default_max_in_flight_bytes 2 * 1024 * 1024 * 1024

  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(args) do
    {name, _args} = Keyword.pop!(args, :name)

    spool_config = Application.get_env(:logflare, :spool, [])
    bucket = Keyword.fetch!(spool_config, :bucket)

    concurrency =
      Keyword.get(spool_config, :consumer_concurrency, max(System.schedulers_online(), 4))

    # Counts segments, not events — needs tuning against real segment-size
    # distribution in production.
    batch_size = Keyword.get(spool_config, :consumer_batch_size, 20)
    queue_name = Keyword.fetch!(spool_config, :queue_name)
    {storage_mod, queue_mod} = ProviderConfig.resolve_mods(spool_config)
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
  def transform(%{segment: segment, handle: handle} = unparsed, _opts) do
    in_flight_ref = QueueProducer.get_in_flight_ref()

    %Message{
      data: unparsed,
      acknowledger:
        {__MODULE__, :noop,
         %{in_flight_ref: in_flight_ref, bytes: byte_size(segment), handle: handle}}
    }
  end

  # Releases one SpoolAck bump per message (= one segment), success or
  # failure, grouped by handle since a batch can span more than one file.
  @impl Broadway.Acknowledger
  def ack(_ack_ref, successful, failed) do
    all = successful ++ failed
    decrement_in_flight(all)
    release_segment_bumps(all)

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

  defp release_segment_bumps(messages) do
    messages
    |> Enum.reduce(%{}, fn message, counts ->
      Map.update(counts, handle_of(message), 1, &(&1 + 1))
    end)
    |> Enum.each(fn {handle, count} -> SpoolAck.ack(handle, count) end)
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

  defp handle_of(%{acknowledger: {_, _, %{} = ack_data}}), do: Map.get(ack_data, :handle)
  defp handle_of(_), do: nil

  # A segment that fails to parse fails just this one message.
  @impl Broadway
  def handle_message(
        _processor,
        %Message{data: %{segment: segment}} = message,
        _context
      ) do
    {duration, result} = :timer.tc(fn -> parse_segment(segment) end)

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

  defp parse_segment(content) do
    {:ok, :erlang.binary_to_term(content)}
  rescue
    e -> {:error, e}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

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
      |> Enum.group_by(&handle_of/1)
      |> Enum.flat_map(fn {handle, handle_messages} ->
        dispatch_handle_group(handle, handle_messages)
      end)
      |> MapSet.new()

    fail_dispatched(messages, failed_source_ids)
  end

  # A batch can span more than one file's segments, so dispatch runs per-handle.
  defp dispatch_handle_group(handle, messages) do
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
        dispatch_group(source_id, records, handle)
    end)
  end

  defp fail_dispatched(messages, failed_source_ids) do
    if Enum.empty?(failed_source_ids) do
      messages
    else
      Enum.map(messages, &fail_message(&1, failed_source_ids))
    end
  end

  defp fail_message(message, failed_source_ids) do
    if Enum.any?(message.data, &MapSet.member?(failed_source_ids, record_source_id(&1))) do
      Message.failed(message, :dispatch_error)
    else
      message
    end
  end

  defp dispatch_group(source_id, lines, handle) do
    case Sources.Cache.get_by(id: source_id) do
      nil ->
        emit_skipped_telemetry(:unknown_source_id, length(lines))

        Logger.debug(
          "spool_consumer: unknown source_id=#{source_id}, skipping #{length(lines)} events"
        )

        []

      source ->
        {:ok, _} = Backends.dispatch_from_spool(lines, source, handle)
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
end
