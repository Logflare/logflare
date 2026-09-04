defmodule Logflare.Backends.Spool.Committer do
  @moduledoc """
  Uploads one sealed WAL segment to GCS/S3 and notifies Pub-Sub/SQS.

  Run inside a `Task` spawned directly by `Logflare.Backends.Spool.Partition`
  (see its moduledoc) rather than owned by a persistent GenServer — every
  rotated segment gets its own concurrent upload, bounded only by
  Partition's `max_inflight_commits`, so one slow commit never blocks the
  next segment's upload from starting. Partition owns the sealed file's
  entire lifecycle (create, seal, delete); this module never touches the
  file except to read it.

  A failed attempt (storage PUT or queue publish) is retried forever with a
  fixed delay between attempts, logging each failure — there is
  deliberately no give-up-and-discard path. The sealed file is durable on
  local disk for as long as it takes; permanently dropping events just
  because GCS/Pub-Sub was unavailable for a while would contradict the
  whole point of this design. A sustained, extended outage accumulates
  un-uploaded sealed files on disk instead — an operational signal to page
  on, not a reason to lose data.
  """

  import Bitwise

  require Logger

  alias Logflare.Backends.Spool.Encoder

  @default_retry_delay_ms 1_000

  @type config :: %{
          bucket: String.t(),
          storage_mod: module(),
          queue_mod: module(),
          queue_ref: String.t() | nil,
          format: :ndjson | :etf,
          compress: boolean(),
          compression_algorithm: :gzip | :zstd,
          index: non_neg_integer()
        }

  @doc """
  Uploads `sealed_path`'s contents and notifies the queue, retrying forever
  on failure. Blocks the calling process until it succeeds, then sends
  `{:commit_result, sealed_path, :ok}` to `partition`.
  """
  @spec commit(pid(), Path.t(), non_neg_integer(), atom(), config()) :: :ok
  def commit(partition, sealed_path, total_count, trigger, config) do
    :ok = do_commit(sealed_path, total_count, trigger, config, 0)
    send(partition, {:commit_result, sealed_path, :ok})
    :ok
  end

  defp do_commit(sealed_path, total_count, trigger, config, attempt) do
    :telemetry.execute(
      [:logflare, :backends, :pipeline, :handle_batch],
      %{batch_size: total_count, batch_trigger: trigger},
      %{backend_type: :spool_producer, batch_trigger: trigger}
    )

    file_key = file_key(config)
    {read_us, body} = :timer.tc(fn -> File.read!(sealed_path) end)

    {io_us, result} =
      :timer.tc(fn ->
        with {:upload, {:ok, _}} <-
               {:upload, config.storage_mod.put(config.bucket, file_key, body, headers(config))},
             {:notify, :ok} <-
               {:notify, notify_queue(config, file_key, total_count)} do
          {:ok, file_key}
        end
      end)

    format_tag = Encoder.format_tag(config.format, config.compress, config.compression_algorithm)
    emit_storage_put_telemetry(format_tag, byte_size(body), result, read_us, io_us)

    case result do
      {:ok, _file_key} ->
        emit_batch_result(:ok, nil, total_count)
        Logger.debug("spool_committer: wrote #{total_count} events to spool", key: file_key)
        :ok

      {stage, {:error, reason}} ->
        emit_batch_result(:error, stage, total_count)

        Logger.error(
          "spool_committer: #{stage} failed key=#{file_key} attempt=#{attempt + 1} " <>
            "error=#{inspect(reason)}"
        )

        Process.sleep(retry_delay_ms())
        do_commit(sealed_path, total_count, trigger, config, attempt + 1)
    end
  end

  defp file_key(config) do
    ext = Encoder.file_extension(config.format, config.compress, config.compression_algorithm)
    "#{config.index}/#{generate_uuidv7()}.v2.#{ext}"
  end

  defp headers(config) do
    base = %{"content-type" => Encoder.content_type(config.format)}

    headers =
      case Encoder.content_encoding(config.compress, config.compression_algorithm) do
        nil -> base
        encoding -> Map.put(base, "content-encoding", encoding)
      end

    [headers: headers]
  end

  defp notify_queue(%{queue_ref: nil}, _file_key, _count), do: :ok

  defp notify_queue(config, file_key, count) do
    body = Jason.encode!(%{file_key: file_key, event_count: count})
    result = config.queue_mod.publish(config.queue_ref, body)

    :telemetry.execute(
      [:logflare, :backends, :spool, :queue, :publish],
      %{count: 1},
      %{result: if(result == :ok, do: :ok, else: :error)}
    )

    case result do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error("spool_committer: queue notify failed for #{file_key}: #{inspect(reason)}")

        {:error, reason}
    end
  end

  defp retry_delay_ms do
    Application.get_env(:logflare, :spool, [])
    |> Keyword.get(:retry_delay_ms, @default_retry_delay_ms)
  end

  defp emit_storage_put_telemetry(format, bytes, result, encode_us, upload_us) do
    :telemetry.execute(
      [:logflare, :backends, :spool, :storage, :put],
      %{count: 1, bytes: bytes, encode_duration: encode_us, upload_duration: upload_us},
      %{format: format, result: if(match?({:ok, _}, result), do: :ok, else: :error)}
    )
  end

  defp emit_batch_result(result, stage, batch_size) do
    :telemetry.execute(
      [:logflare, :backends, :spool, :producer, :batch],
      %{count: batch_size},
      %{result: result, stage: stage}
    )
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
