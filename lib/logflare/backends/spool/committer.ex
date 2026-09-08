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

  A failed upload/notify attempt is retried up to `max_commit_attempts/0`
  times (config, default 5), logging each failure, then gives up — retrying
  forever isn't worth the liability of an unbounded loop, and giving up
  doesn't lose the file: Partition only ever deletes it on success, so it's
  still on disk for the next restart's crash-recovery scan to find and
  retry again.

  A sealed file that can't even be *read*, or whose frames fail CRC
  validation (see `Logflare.Backends.Spool.Framing`), is a different
  failure class, though: unlike an upload failing, retrying either can
  never succeed — the bytes on disk aren't going to change — so both log
  loudly, emit telemetry, and give up on that one segment immediately
  rather than spending any retry attempts on it. Checking frames here,
  before ever uploading, catches disk corruption at the source instead of
  only downstream when a consumer eventually fails to decode the file.
  """

  import Bitwise

  require Logger

  alias Logflare.Backends.Spool.Encoder
  alias Logflare.Backends.Spool.Framing

  @default_retry_delay_ms 1_000
  @default_max_commit_attempts 5

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
  Spawns an unlinked `Task` that uploads `sealed_path`'s contents and
  notifies the queue, retrying up to `max_commit_attempts/0` times on an
  upload/notify failure, then reports the result back to `partition` as
  `{:commit_result, sealed_path, result}`. Returns the task's pid so the
  caller can monitor it — a crashing commit is `partition`'s concern (see
  its `:DOWN` handling), not this module's.
  """
  @spec commit_async(pid(), Path.t(), non_neg_integer(), atom(), config()) :: {:ok, pid()}
  def commit_async(partition, sealed_path, total_count, trigger, config) do
    Task.start(fn ->
      result = do_commit(sealed_path, total_count, trigger, config, 0)
      send(partition, {:commit_result, sealed_path, result})
    end)
  end

  defp do_commit(sealed_path, total_count, trigger, config, attempt) do
    with {:ok, body} <- File.read(sealed_path),
         {:ok, _segments} <- Framing.decode_segments(body) do
      commit_body(sealed_path, body, total_count, trigger, config, attempt)
    else
      {:error, reason} ->
        :telemetry.execute(
          [:logflare, :backends, :spool, :committer, :read_error],
          %{count: 1},
          %{reason: reason}
        )

        Logger.error(
          "spool_committer: sealed file unreadable, dropping #{total_count} events at " <>
            "#{sealed_path}: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  defp commit_body(sealed_path, body, total_count, trigger, config, attempt) do
    case upload_and_notify(body, config, total_count, trigger) do
      {:ok, file_key} ->
        Logger.debug("spool_committer: wrote #{total_count} events to spool", key: file_key)
        {:ok, file_key}

      {:error, {stage, reason}} ->
        max_attempts = max_commit_attempts()

        Logger.error(
          "spool_committer: #{stage} failed path=#{sealed_path} " <>
            "attempt=#{attempt + 1}/#{max_attempts} error=#{inspect(reason)}"
        )

        if attempt + 1 < max_attempts do
          Process.sleep(retry_delay_ms())
          do_commit(sealed_path, total_count, trigger, config, attempt + 1)
        else
          {:error, reason}
        end
    end
  end

  defp upload_and_notify(body, config, total_count, trigger) do
    :telemetry.execute(
      [:logflare, :backends, :pipeline, :handle_batch],
      %{batch_size: total_count, batch_trigger: trigger},
      %{backend_type: :spool_producer, batch_trigger: trigger}
    )

    file_key = file_key(config)

    with {:upload, {:ok, _}} <-
           {:upload, config.storage_mod.put(config.bucket, file_key, body, headers(config))},
         {:notify, :ok} <- {:notify, notify_queue(config, file_key, total_count)} do
      emit_batch_result(:ok, nil, total_count)
      {:ok, file_key}
    else
      {stage, {:error, reason}} ->
        emit_batch_result(:error, stage, total_count)
        {:error, {stage, reason}}
    end
  end

  defp file_key(config) do
    ext = Encoder.file_extension(config.format, config.compress, config.compression_algorithm)
    Encoder.file_key_with_version("#{config.index}/#{generate_uuidv7()}", ext)
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

  defp max_commit_attempts do
    Application.get_env(:logflare, :spool, [])
    |> Keyword.get(:max_commit_attempts, @default_max_commit_attempts)
  end

  defp retry_delay_ms do
    Application.get_env(:logflare, :spool, [])
    |> Keyword.get(:retry_delay_ms, @default_retry_delay_ms)
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
