defmodule Logflare.Backends.Spool.ProducerPipeline do
  @moduledoc false

  use Broadway

  import Bitwise

  require Logger

  alias Broadway.Message
  alias Logflare.Backends.BufferProducer
  alias Logflare.Backends.Spool.MemoryMonitor
  alias Logflare.Backends.Spool.Storage
  alias Logflare.Backends.Spool.Queue

  @behaviour Broadway.Acknowledger

  @max_batch_size 500_000
  @default_batch_timeout 5_000
  @max_spool_file_size 32 * 1024 * 1024
  @early_flush_file_size 12 * 1024 * 1024

  @processor_concurrency 6
  @batcher_concurrency 4
  @producer_concurrency 1


  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(args) do
    dbg("Started spool producer")
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
    for %{data: {id, tid, _size}} <- successful ++ failed do
      :ets.delete(tid, id)
    end

    :ok
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
    dbg("SpoolProducerPipeline handle_batch #{Enum.count(messages)}")
    dbg(batch_info)
    dbg(Enum.take(messages,1))

    partition = :rand.uniform(partitions) - 1
    file_key = "#{partition}/#{generate_uuidv7()}.#{file_extension(format, compress)}"
    result = do_upload(format, compress, messages, bucket, file_key, storage_mod)

    case result do
      {:ok, _} ->
        :telemetry.execute(
          [:logflare, :backends, :pipeline, :handle_batch],
          %{batch_size: batch_info.size, batch_trigger: batch_info.trigger},
          %{backend_type: :spool_producer}
        )

        notify_queue(queue_mod, queue_ref, file_key, batch_info.size)

        Logger.debug("spool_producer_pipeline: wrote #{batch_info.size} events to spool",
          key: file_key
        )

        messages

      {:error, reason} ->
        Logger.error("spool_producer_pipeline: write failed key=#{file_key} error=#{inspect(reason)}")

        Enum.map(messages, &Message.failed(&1, reason))
    end
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

        %{data: {_id, _tid, size}}, {count, :pending} ->
          budget = effective_max_file_size()
          dbg({:spool_batch_budget, budget, :early_flush?, budget != @max_spool_file_size})
          continue_or_emit(size, count, budget)

        %{data: {_id, _tid, size}}, {count, budget} ->
          continue_or_emit(size, count, budget)
      end
    }
  end

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
    {serialize_us, body} = :timer.tc(fn -> compress_to_binary(messages) end)
    dbg({:serialize_ms, Float.round(serialize_us / 1000, 1), :format, :ndjson_gz, :bytes, byte_size(body)})

    {upload_us, result} = :timer.tc(fn ->
      storage_mod.put(bucket, file_key, body,
        headers: %{"content-type" => "application/x-ndjson", "content-encoding" => "gzip"}
      )
    end)
    dbg({:upload_ms, Float.round(upload_us / 1000, 1), file_key})
    result
  end

  defp upload_plain(messages, bucket, file_key, storage_mod) do
    {serialize_us, body} =
      :timer.tc(fn ->
        Enum.flat_map(messages, fn %{data: {id, tid, _size}} ->
          case :ets.lookup(tid, id) do
            [{^id, _status, log_event, _byte_size}] -> [encode_line(log_event), "\n"]
            [] -> []
          end
        end)
        |> IO.iodata_to_binary()
      end)

    dbg({:serialize_ms, Float.round(serialize_us / 1000, 1), :format, :ndjson, :bytes, byte_size(body)})

    {upload_us, result} = :timer.tc(fn ->
      storage_mod.put(bucket, file_key, body, headers: %{"content-type" => "application/x-ndjson"})
    end)
    dbg({:upload_ms, Float.round(upload_us / 1000, 1), file_key})
    result
  end

  defp upload_etf(compress, messages, bucket, file_key, storage_mod) do
    {serialize_us, body} =
      :timer.tc(fn ->
        records =
          Enum.flat_map(messages, fn %{data: {id, tid, _size}} ->
            case :ets.lookup(tid, id) do
              [{^id, _status, log_event, _byte_size}] ->
                [%{
                  id: log_event.id,
                  source_id: log_event.source_id,
                  body: log_event.body,
                  event_type: log_event.event_type,
                  ingested_at: DateTime.to_unix(log_event.ingested_at, :microsecond)
                }]

              [] ->
                []
            end
          end)

        body = :erlang.term_to_binary(records)
        if compress, do: gzip(body), else: body
      end)

    dbg({:serialize_ms, Float.round(serialize_us / 1000, 1), :format, (if compress, do: :etf_gz, else: :etf), :bytes, byte_size(body)})

    {upload_us, result} = :timer.tc(fn ->
      storage_mod.put(bucket, file_key, body, headers: %{"content-type" => "application/octet-stream"})
    end)
    dbg({:upload_ms, Float.round(upload_us / 1000, 1), file_key})
    result
  end

  defp compress_to_binary(messages) do
    z = :zlib.open()

    try do
      :ok = :zlib.deflateInit(z, :default, :deflated, 31, 8, :default)

      chunks =
        Enum.flat_map(messages, fn %{data: {id, tid, _size}} ->
          case :ets.lookup(tid, id) do
            [{^id, _status, log_event, _byte_size}] ->
              :zlib.deflate(z, [encode_line(log_event), "\n"], :none)

            [] ->
              []
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
      ingested_at: log_event.ingested_at
    })
  end

  defp notify_queue(_queue_mod, nil, _file_key, _count), do: :ok

  defp notify_queue(queue_mod, queue_ref, file_key, count) do
    body = Jason.encode!(%{file_key: file_key, event_count: count})

    case queue_mod.publish(queue_ref, body) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error("spool_producer_pipeline: queue notify failed for #{file_key}: #{inspect reason}")
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
            Logger.warning("spool_producer_pipeline: could not resolve queue ref for #{queue_name}: #{inspect(reason)}")
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
