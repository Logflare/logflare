defmodule Logflare.Backends.S3ProducerPipeline do
  @moduledoc false

  use Broadway

  import Bitwise

  require Logger

  alias Broadway.Message
  alias Logflare.Backends.BufferProducer

  @behaviour Broadway.Acknowledger

  @max_batch_size 500_000
  @default_batch_timeout 5_000
  @max_s3_file_size 64 * 1024 * 1024

  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(args) do
    {name, _args} = Keyword.pop!(args, :name)

    s3_config = Application.get_env(:logflare, :s3_spool, [])
    bucket = Keyword.fetch!(s3_config, :bucket)
    partitions = Keyword.get(s3_config, :partitions, 4)
    batch_timeout = Keyword.get(s3_config, :batch_timeout, @default_batch_timeout)
    compress = Keyword.get(s3_config, :compress, true)

    Broadway.start_link(__MODULE__,
      name: name,
      hibernate_after: 5_000,
      spawn_opt: [fullsweep_after: 10],
      producer: [
        module: {BufferProducer, [s3_producer: true, id_passing: true]},
        transformer: {__MODULE__, :transform, []}
      ],
      processors: [default: [concurrency: 8, max_demand: 1_000]],
      batchers: [
        s3: [
          concurrency: partitions,
          batch_size: s3_batch_size_splitter(),
          batch_timeout: batch_timeout,
          max_demand: @max_batch_size
        ]
      ],
      context: %{
        bucket: bucket,
        partitions: partitions,
        compress: compress
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
    Message.put_batcher(message, :s3)
  end

  @impl Broadway
  def handle_batch(:s3, messages, batch_info, %{
        bucket: bucket,
        partitions: partitions,
        compress: compress
      }) do
    dbg("S3ProducerPipeline handle_batch #{Enum.count(messages)}")
    dbg(batch_info)

    partition = :rand.uniform(partitions) - 1

    {file_key, result} =
      if compress do
        key = "#{partition}/#{generate_uuidv7()}.ndjson.gz"
        {key, upload_compressed(messages, bucket, key)}
      else
        key = "#{partition}/#{generate_uuidv7()}.ndjson"
        {key, upload_plain(messages, bucket, key)}
      end

    case result do
      {:ok, _} ->
        :telemetry.execute(
          [:logflare, :backends, :pipeline, :handle_batch],
          %{batch_size: batch_info.size, batch_trigger: batch_info.trigger},
          %{backend_type: :s3_producer}
        )

        Logger.debug("s3_producer_pipeline: wrote #{batch_info.size} events to s3",
          key: file_key
        )

        messages

      {:error, reason} ->
        Logger.error("s3_producer_pipeline: S3 write failed",
          key: file_key,
          error: inspect(reason)
        )

        Enum.map(messages, &Message.failed(&1, reason))
    end
  end

  @spec s3_batch_size_splitter() ::
          {{non_neg_integer(), non_neg_integer()},
           (Message.t(), {non_neg_integer(), non_neg_integer()} ->
              {:emit | :cont, {non_neg_integer(), non_neg_integer()}})}
  defp s3_batch_size_splitter do
    {
      {@max_batch_size, @max_s3_file_size},
      fn
        _message, {1, _remaining} ->
          {:emit, {@max_batch_size, @max_s3_file_size}}

        %{data: {_id, _tid, size}}, {count, remaining} ->
          if remaining - size <= 0 do
            {:emit, {@max_batch_size, @max_s3_file_size}}
          else
            {:cont, {count - 1, remaining - size}}
          end
      end
    }
  end

  defp upload_compressed(messages, bucket, file_key) do
    body = compress_to_binary(messages)

    ExAws.S3.put_object(bucket, file_key, body,
      headers: %{"content-type" => "application/x-ndjson", "content-encoding" => "gzip"}
    )
    |> ExAws.request()
  end

  defp upload_plain(messages, bucket, file_key) do
    body =
      Enum.flat_map(messages, fn %{data: {id, tid, _size}} ->
        case :ets.lookup(tid, id) do
          [{^id, _status, log_event, _byte_size}] -> [encode_line(log_event), "\n"]
          [] -> []
        end
      end)
      |> IO.iodata_to_binary()

    ExAws.S3.put_object(bucket, file_key, body,
      headers: %{"content-type" => "application/x-ndjson"}
    )
    |> ExAws.request()
  end

  defp compress_to_binary(messages) do
    z = :zlib.open()
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
    :zlib.close(z)

    IO.iodata_to_binary([chunks, final])
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
