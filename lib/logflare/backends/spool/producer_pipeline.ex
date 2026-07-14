defmodule Logflare.Backends.Spool.ProducerPipeline do
  @moduledoc false

  use Broadway

  import Bitwise

  require Logger

  alias Broadway.Message
  alias Logflare.Backends.BufferProducer
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.IngestEventQueue.LogEventPointer
  alias Logflare.Backends.Spool.MemoryMonitor
  alias Logflare.Backends.Spool.Storage
  alias Logflare.Backends.Spool.Queue

  @behaviour Broadway.Acknowledger

  @max_batch_size 500_000
  @default_batch_timeout 5_000
  @max_spool_file_size 32 * 1024 * 1024
  @early_flush_file_size 12 * 1024 * 1024
  @default_max_retries 0

  @processor_concurrency 6
  @batcher_concurrency 4
  @producer_concurrency 1

  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(args) do
    {name, _args} = Keyword.pop!(args, :name)

    spool_config = Application.get_env(:logflare, :spool, [])
    bucket = Keyword.fetch!(spool_config, :bucket)
    partitions = Keyword.get(spool_config, :partitions, 4)
    batch_timeout = Keyword.get(spool_config, :batch_timeout, @default_batch_timeout)
    compress = Keyword.get(spool_config, :compress, true)
    format = Keyword.get(spool_config, :format, :ndjson)
    {storage_mod, queue_mod} = resolve_mods(spool_config)
    queue_ref = resolve_queue_ref(spool_config, queue_mod)

    Broadway.start_link(__MODULE__,
      name: name,
      hibernate_after: 5_000,
      spawn_opt: [fullsweep_after: 10],
      producer: [
        module: {BufferProducer, [spool_producer: true, id_passing: true]},
        transformer: {__MODULE__, :transform, []},
        concurrency: @producer_concurrency
      ],
      processors: [default: [concurrency: @processor_concurrency, max_demand: 1_000]],
      batchers: [
        spool: [
          concurrency: @batcher_concurrency,
          batch_size: spool_batch_size_splitter(),
          batch_timeout: batch_timeout,
          max_demand: @max_batch_size
        ]
      ],
      context: %{
        bucket: bucket,
        partitions: partitions,
        compress: compress,
        format: format,
        queue_ref: queue_ref,
        storage_mod: storage_mod,
        queue_mod: queue_mod
      }
    )
  end

  @spec transform(term(), keyword()) :: Message.t()
  def transform(event, _opts) do
    %Message{
      data: event,
      acknowledger: {__MODULE__, :no_ack_ref, :ack_data}
    }
  end

  @impl Broadway.Acknowledger
  def ack(_ref, successful, failed) do
    # The pointer was already removed from its queue at claim time
    # (take_pending_pointers/2 claims via :ets.take/2); ack still has to delete the
    # event row from the generation store itself — GenerationJanitor's rotation is a
    # failsafe for abandoned claims, not the primary cleanup path.
    Enum.each(successful, fn %{data: %LogEventPointer{} = pointer} ->
      IngestEventQueue.delete_id(pointer.tid, pointer.id)
    end)

    maybe_requeue_failed(failed)

    :ok
  end

  # Requeue failed spool writes for retry — same bounded-retry pattern as
  # Source.BigQuery.Pipeline.maybe_requeue_failed/3. Every failed message's pointer was
  # already removed from its queue at claim time — retriable ones are written back
  # directly into the queue they were claimed from (see
  # IngestEventQueue.reinsert_pointer/1); exhausted ones still need their event row
  # deleted from the generation store, since nothing will ever ack them.
  defp maybe_requeue_failed([]), do: :ok

  defp maybe_requeue_failed(failed) do
    max_retries = spool_max_retries()

    {retriable, exhausted} =
      failed
      |> Enum.map(fn %{data: %LogEventPointer{} = pointer} -> pointer end)
      |> Enum.split_with(&(&1.retries < max_retries))

    if exhausted != [] do
      Logger.warning(
        "spool_producer_pipeline: dropping #{length(exhausted)} events: exhausted #{max_retries} retries"
      )

      Enum.each(exhausted, fn pointer -> IngestEventQueue.delete_id(pointer.tid, pointer.id) end)
    end

    requeue_retriable(retriable)
  end

  defp requeue_retriable([]), do: :ok

  defp requeue_retriable(retriable) do
    Logger.warning(
      "spool_producer_pipeline: requeuing #{length(retriable)} failed events for retry"
    )

    Enum.each(retriable, fn pointer ->
      IngestEventQueue.reinsert_pointer(%{pointer | retries: pointer.retries + 1})
    end)
  end

  defp spool_max_retries do
    Application.get_env(:logflare, :spool, [])
    |> Keyword.get(:max_retries, @default_max_retries)
  end

  @impl Broadway
  def handle_message(_processor, message, _context) do
    Message.put_batcher(message, :spool)
  end

  @impl Broadway
  def handle_batch(:spool, messages, batch_info, %{
        bucket: bucket,
        partitions: partitions,
        compress: compress,
        format: format,
        queue_ref: queue_ref,
        storage_mod: storage_mod,
        queue_mod: queue_mod
      }) do
    :telemetry.execute(
      [:logflare, :backends, :pipeline, :handle_batch],
      %{batch_size: batch_info.size, batch_trigger: batch_info.trigger},
      %{backend_type: :spool_producer}
    )

    partition = :rand.uniform(partitions) - 1
    file_key = "#{partition}/#{generate_uuidv7()}.#{file_extension(format, compress)}"

    with {:upload, {:ok, _}} <-
           {:upload, do_upload(format, compress, messages, bucket, file_key, storage_mod)},
         {:notify, :ok} <-
           {:notify, notify_queue(queue_mod, queue_ref, file_key, batch_info.size)} do
      emit_batch_result(:ok, nil, batch_info.size)

      Logger.debug("spool_producer_pipeline: wrote #{batch_info.size} events to spool",
        key: file_key
      )

      messages
    else
      {stage, {:error, reason}} ->
        # On a :notify failure the file is already durably written at
        # file_key — this only means nothing was told to go fetch it.
        # Marking failed re-uploads the same events under a fresh key on
        # retry (see maybe_requeue_failed/1) rather than leaving this batch
        # stuck at a file nothing will ever be notified about; the orphaned
        # file at the old key is cleaned up by the bucket's lifecycle policy.
        emit_batch_result(:error, stage, batch_info.size)

        Logger.error(
          "spool_producer_pipeline: #{stage} failed key=#{file_key} error=#{inspect(reason)}"
        )

        Enum.map(messages, &Message.failed(&1, reason))
    end
  end

  defp emit_batch_result(result, stage, batch_size) do
    :telemetry.execute(
      [:logflare, :backends, :spool, :producer, :batch],
      %{count: batch_size},
      %{result: result, stage: stage}
    )
  end

  # Throttle state is sampled once per batch (on the first message after a
  # reset), not once per message — the :pending sentinel marks "not yet
  # decided for this batch". Logflare.Backends.Spool.MemoryMonitor.throttled?/0
  # is a cheap :persistent_term read, but even that isn't worth paying on
  # every single message when a batch can be up to @max_batch_size messages.
  #
  # Public (not private) so the returned {initial_acc, reducer_fn} tuple can
  # be exercised directly in tests without spinning up a full Broadway pipeline.
  @doc false
  @spec spool_batch_size_splitter() ::
          {{non_neg_integer(), non_neg_integer() | :pending},
           (Message.t(), {non_neg_integer(), non_neg_integer() | :pending} ->
              {:emit | :cont, {non_neg_integer(), non_neg_integer() | :pending}})}
  def spool_batch_size_splitter do
    {
      {@max_batch_size, :pending},
      fn
        _message, {1, _budget} ->
          {:emit, {@max_batch_size, :pending}}

        message, {count, :pending} ->
          budget = effective_max_file_size()
          continue_or_emit(message_size(message), count, budget)

        message, {count, budget} ->
          continue_or_emit(message_size(message), count, budget)
      end
    }
  end

  defp message_size(%{data: %LogEventPointer{size: size}}), do: size

  @spec continue_or_emit(non_neg_integer(), non_neg_integer(), non_neg_integer()) ::
          {:emit | :cont, {non_neg_integer(), non_neg_integer() | :pending}}
  defp continue_or_emit(size, count, budget) do
    if budget - size <= 0 do
      {:emit, {@max_batch_size, :pending}}
    else
      {:cont, {count - 1, budget - size}}
    end
  end

  @spec effective_max_file_size() :: non_neg_integer()
  defp effective_max_file_size do
    if MemoryMonitor.throttled?() do
      @early_flush_file_size
    else
      @max_spool_file_size
    end
  end

  defp file_extension(:ndjson, true), do: "ndjson.gz"
  defp file_extension(:ndjson, false), do: "ndjson"
  defp file_extension(:etf, true), do: "etf.gz"
  defp file_extension(:etf, false), do: "etf"

  defp do_upload(:ndjson, true, messages, bucket, file_key, storage_mod),
    do: upload_compressed(messages, bucket, file_key, storage_mod)

  defp do_upload(:ndjson, false, messages, bucket, file_key, storage_mod),
    do: upload_plain(messages, bucket, file_key, storage_mod)

  defp do_upload(:etf, compress, messages, bucket, file_key, storage_mod),
    do: upload_etf(compress, messages, bucket, file_key, storage_mod)

  defp upload_compressed(messages, bucket, file_key, storage_mod) do
    body = compress_to_binary(messages)

    result =
      storage_mod.put(bucket, file_key, body,
        headers: %{"content-type" => "application/x-ndjson", "content-encoding" => "gzip"}
      )

    emit_storage_put_telemetry(:ndjson_gz, byte_size(body), result)
    result
  end

  defp lookup_message_event(%{data: %LogEventPointer{tid: tid, id: id}}) do
    IngestEventQueue.lookup_event(tid, id)
  end

  defp upload_plain(messages, bucket, file_key, storage_mod) do
    body =
      Enum.flat_map(messages, fn message ->
        case lookup_message_event(message) do
          nil -> []
          log_event -> [encode_line(log_event), "\n"]
        end
      end)
      |> IO.iodata_to_binary()

    result =
      storage_mod.put(bucket, file_key, body,
        headers: %{"content-type" => "application/x-ndjson"}
      )

    emit_storage_put_telemetry(:ndjson, byte_size(body), result)
    result
  end

  defp upload_etf(compress, messages, bucket, file_key, storage_mod) do
    records =
      Enum.flat_map(messages, fn message ->
        case lookup_message_event(message) do
          nil ->
            []

          log_event ->
            [
              %{
                id: log_event.id,
                source_id: log_event.source_id,
                body: log_event.body,
                event_type: log_event.event_type,
                ingested_at: DateTime.to_unix(log_event.ingested_at, :microsecond),
                via_rule_id: log_event.via_rule_id
              }
            ]
        end
      end)

    body = :erlang.term_to_binary(records)
    body = if compress, do: gzip(body), else: body

    result =
      storage_mod.put(bucket, file_key, body,
        headers: %{"content-type" => "application/octet-stream"}
      )

    emit_storage_put_telemetry(if(compress, do: :etf_gz, else: :etf), byte_size(body), result)
    result
  end

  defp emit_storage_put_telemetry(format, bytes, result) do
    :telemetry.execute(
      [:logflare, :backends, :spool, :storage, :put],
      %{count: 1, bytes: bytes},
      %{format: format, result: if(match?({:ok, _}, result), do: :ok, else: :error)}
    )
  end

  defp compress_to_binary(messages) do
    z = :zlib.open()

    try do
      :ok = :zlib.deflateInit(z, :default, :deflated, 31, 8, :default)

      chunks =
        Enum.flat_map(messages, fn message ->
          case lookup_message_event(message) do
            nil -> []
            log_event -> :zlib.deflate(z, [encode_line(log_event), "\n"], :none)
          end
        end)

      final = :zlib.deflate(z, [], :finish)
      :zlib.deflateEnd(z)

      IO.iodata_to_binary([chunks, final])
    after
      # Broadway rescues exceptions raised from handle_batch/4 without crashing
      # this process, so the port is never closed by BEAM's automatic
      # close-on-owner-death cleanup either — without this `after`, a raise
      # partway through deflate leaks the port permanently.
      :zlib.close(z)
    end
  end

  defp gzip(data) do
    z = :zlib.open()

    try do
      :ok = :zlib.deflateInit(z, :default, :deflated, 31, 8, :default)
      chunks = :zlib.deflate(z, data, :finish)
      :zlib.deflateEnd(z)
      IO.iodata_to_binary(chunks)
    after
      :zlib.close(z)
    end
  end

  defp encode_line(log_event) do
    Jason.encode!(%{
      id: log_event.id,
      source_id: log_event.source_id,
      body: log_event.body,
      event_type: log_event.event_type,
      ingested_at: log_event.ingested_at,
      via_rule_id: log_event.via_rule_id
    })
  end

  defp notify_queue(_queue_mod, nil, _file_key, _count), do: :ok

  defp notify_queue(queue_mod, queue_ref, file_key, count) do
    body = Jason.encode!(%{file_key: file_key, event_count: count})
    result = queue_mod.publish(queue_ref, body)

    :telemetry.execute(
      [:logflare, :backends, :spool, :queue, :publish],
      %{count: 1},
      %{result: if(result == :ok, do: :ok, else: :error)}
    )

    case result do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error(
          "spool_producer_pipeline: queue notify failed for #{file_key}: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  defp resolve_queue_ref(spool_config, queue_mod) do
    # For PubSub: producer publishes to a topic, consumer reads from a subscription.
    # pubsub_topic overrides queue_name when set (GCP producer mode).
    name = Keyword.get(spool_config, :pubsub_topic) || Keyword.get(spool_config, :queue_name)

    case name do
      nil ->
        nil

      queue_name ->
        case queue_mod.resolve(queue_name) do
          {:ok, ref} ->
            ref

          {:error, reason} ->
            Logger.warning(
              "spool_producer_pipeline: could not resolve queue ref for #{queue_name}: #{inspect(reason)}"
            )

            nil
        end
    end
  end

  defp resolve_mods(spool_config) do
    provider = Keyword.get(spool_config, :provider, :aws)
    storage_mod = Keyword.get(spool_config, :storage_mod, default_storage_mod(provider))
    queue_mod = Keyword.get(spool_config, :queue_mod, default_queue_mod(provider))
    {storage_mod, queue_mod}
  end

  defp default_storage_mod(:gcp), do: Storage.GCS
  defp default_storage_mod(_), do: Storage.S3

  defp default_queue_mod(:gcp), do: Queue.PubSub
  defp default_queue_mod(_), do: Queue.SQS

  @spec generate_uuidv7() :: String.t()
  defp generate_uuidv7 do
    ms = System.system_time(:millisecond)

    <<rand_a::12, _::4>> = :crypto.strong_rand_bytes(2)
    <<_::2, rand_b::62>> = :crypto.strong_rand_bytes(8)
    <<time_high::32, time_mid::16>> = <<ms::48>>

    ver_rand_a = 0x7000 ||| rand_a
    var_rand_b = 0x8000_0000_0000_0000 ||| rand_b

    hex = fn n, len ->
      n |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(len, "0")
    end

    node = var_rand_b |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(16, "0")
    {clock_seq, node_str} = String.split_at(node, 4)

    "#{hex.(time_high, 8)}-#{hex.(time_mid, 4)}-#{hex.(ver_rand_a, 4)}-#{clock_seq}-#{node_str}"
  end
end
