defmodule Logflare.Backends.Spool.ConsumerPipeline do
  @moduledoc false

  use Broadway

  require Logger

  alias Broadway.Message
  alias Logflare.Backends
  alias Logflare.Backends.Spool.ConsumerPipeline.QueueProducer
  alias Logflare.Backends.Spool.Queue
  alias Logflare.Backends.Spool.Storage
  alias Logflare.Sources

  @behaviour Broadway.Acknowledger

  # Generous safety valve (2x total batcher capacity), not a fine-grained
  # flow-control knob — same ratio as the ClickHouse/spool producer pipelines.
  @max_in_flight_multiplier 2

  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(args) do
    {name, _args} = Keyword.pop!(args, :name)

    spool_config = Application.get_env(:logflare, :spool, [])
    bucket = Keyword.fetch!(spool_config, :bucket)

    concurrency =
      Keyword.get(spool_config, :consumer_concurrency, max(System.schedulers_online(), 4))

    batch_size = Keyword.get(spool_config, :consumer_batch_size, 500)
    queue_name = Keyword.fetch!(spool_config, :queue_name)
    provider = Keyword.get(spool_config, :provider, :aws)
    storage_mod = Keyword.get(spool_config, :storage_mod, default_storage_mod(provider))
    queue_mod = Keyword.get(spool_config, :queue_mod, default_queue_mod(provider))
    queue_url = resolve_queue_url!(queue_name, queue_mod)
    max_in_flight = @max_in_flight_multiplier * batch_size * concurrency

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
        default: [concurrency: concurrency, min_demand: 50, max_demand: 500]
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
  def transform(line, _opts) do
    in_flight_ref = QueueProducer.get_in_flight_ref()

    %Message{
      data: line,
      acknowledger: {__MODULE__, :noop, %{in_flight_ref: in_flight_ref}}
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
    |> Enum.group_by(&in_flight_ref_of/1)
    |> Enum.each(fn
      {nil, _msgs} -> :ok
      {ref, msgs} -> :atomics.sub(ref, 1, length(msgs))
    end)
  end

  # Tolerates the shapes hand-built test messages use (acknowledger: nil, or
  # a {mod, ref, nil} placeholder) as well as the real transform/2 output.
  defp in_flight_ref_of(%{acknowledger: {_, _, %{} = ack_data}}),
    do: Map.get(ack_data, :in_flight_ref)

  defp in_flight_ref_of(_), do: nil

  @impl Broadway
  def handle_message(_processor, %Message{} = message, _context) do
    message
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

    messages
    |> Enum.group_by(&record_source_id(&1.data), & &1.data)
    |> Enum.each(fn
      {nil, lines} ->
        emit_skipped_telemetry(:missing_source_id, length(lines))
        Logger.debug("spool_consumer: #{length(lines)} events missing source_id, skipping")

      {source_id, lines} ->
        dispatch_group(source_id, lines)
    end)

    messages
  end

  defp dispatch_group(source_id, lines) do
    case Sources.Cache.get_by(id: source_id) do
      nil ->
        emit_skipped_telemetry(:unknown_source_id, length(lines))

        Logger.debug(
          "spool_consumer: unknown source_id=#{source_id}, skipping #{length(lines)} events"
        )

      source ->
        {:ok, _} = Backends.dispatch_from_spool(lines, source)
        :ok
    end
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
