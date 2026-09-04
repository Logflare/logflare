defmodule Logflare.Backends.Spool.Committer do
  @moduledoc """
  Uploads spool segments to GCS/S3 and notifies Pub-Sub/SQS — source-
  agnostic: a `source()` is either an already-sealed WAL file on disk
  (`{:file, path}`) or an already-framed segment still only in memory
  (`{:body, bytes}`, used by `Logflare.Backends.Spool.Partition`'s
  disk-write-failure fallback — see its moduledoc). Both are read down to
  the same bytes and go through the exact same upload+notify step; nothing
  past `read/1` needs to know or care which kind a commit started as.

  `commit/4` is the blocking primitive: up to `max_commit_attempts/0`
  (config, default 5) attempts, logging each failure, then gives up —
  retrying forever isn't worth the liability of an unbounded loop, and a
  file is never lost by giving up on it here: `Partition` only ever
  deletes a `{:file, _}` source on success, so an exhausted one is still
  on disk for the next restart's crash-recovery scan to find and retry
  again. A `{:body, _}` source has no such backup — losing it after
  exhausted retries is the already-accepted risk of the disk-write-failure
  fallback that produced it in the first place, not a new one introduced
  here.

  `commit_async/5` spawns an unlinked `Task` around `commit/4` and reports
  the result back to `partition` as `{:commit_result, source, result}` —
  used for every commit that shouldn't block a caller (a normal rotation,
  a recovered file, or the async disk-fallback).
  """

  import Bitwise

  require Logger

  alias Logflare.Backends.Spool.Encoder

  @default_retry_delay_ms 1_000
  @default_max_commit_attempts 5

  @type source :: {:file, Path.t()} | {:body, binary()}

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
  Uploads `source` and notifies the queue, retrying up to
  `max_commit_attempts/0` times on an upload/notify failure. Blocks the
  calling process until it settles. A read failure (a `{:file, _}` source
  that's unreadable) is never retried — logged, telemetered, and given up
  on immediately, since retrying a read that will never succeed can't help.
  """
  @spec commit(source(), non_neg_integer(), atom(), config()) ::
          {:ok, file_key :: String.t()} | {:error, term()}
  def commit(source, total_count, trigger, config) do
    do_commit(source, total_count, trigger, config, 0)
  end

  @doc """
  Spawns an unlinked `Task` running `commit/4` and reports its result back
  to `partition` as `{:commit_result, source, result}`. Returns the task's
  pid so the caller can monitor it — a crashing commit is `partition`'s
  concern (see its `:DOWN` handling), not this module's.
  """
  @spec commit_async(pid(), source(), non_neg_integer(), atom(), config()) :: {:ok, pid()}
  def commit_async(partition, source, total_count, trigger, config) do
    Task.start(fn ->
      result = commit(source, total_count, trigger, config)
      send(partition, {:commit_result, source, result})
    end)
  end

  defp do_commit(source, total_count, trigger, config, attempt) do
    case read(source) do
      {:ok, body} ->
        case upload_and_notify(body, config, total_count, trigger) do
          {:ok, file_key} ->
            Logger.debug("spool_committer: wrote #{total_count} events to spool", key: file_key)
            {:ok, file_key}

          {:error, {stage, reason}} ->
            max_attempts = max_commit_attempts()

            Logger.error(
              "spool_committer: #{stage} failed source=#{inspect(source)} " <>
                "attempt=#{attempt + 1}/#{max_attempts} error=#{inspect(reason)}"
            )

            if attempt + 1 < max_attempts do
              Process.sleep(retry_delay_ms())
              do_commit(source, total_count, trigger, config, attempt + 1)
            else
              {:error, reason}
            end
        end

      {:error, reason} ->
        :telemetry.execute(
          [:logflare, :backends, :spool, :committer, :read_error],
          %{count: 1},
          %{reason: reason}
        )

        Logger.error(
          "spool_committer: source unreadable, dropping #{total_count} events: " <>
            "#{inspect(source)}: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  defp read({:file, path}), do: File.read(path)
  defp read({:body, body}), do: {:ok, body}

  defp upload_and_notify(body, config, total_count, trigger) do
    :telemetry.execute(
      [:logflare, :backends, :pipeline, :handle_batch],
      %{batch_size: total_count, batch_trigger: trigger},
      %{backend_type: :spool_producer, batch_trigger: trigger}
    )

    file_key = file_key(config)

    {upload_us, result} =
      :timer.tc(fn ->
        with {:upload, {:ok, _}} <-
               {:upload, config.storage_mod.put(config.bucket, file_key, body, headers(config))},
             {:notify, :ok} <-
               {:notify, notify_queue(config, file_key, total_count)} do
          {:ok, file_key}
        end
      end)

    format_tag = Encoder.format_tag(config.format, config.compress, config.compression_algorithm)
    emit_storage_put_telemetry(format_tag, byte_size(body), result, upload_us)

    case result do
      {:ok, file_key} ->
        emit_batch_result(:ok, nil, total_count)
        {:ok, file_key}

      {stage, {:error, reason}} ->
        emit_batch_result(:error, stage, total_count)
        {:error, {stage, reason}}
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

  defp max_commit_attempts do
    Application.get_env(:logflare, :spool, [])
    |> Keyword.get(:max_commit_attempts, @default_max_commit_attempts)
  end

  defp retry_delay_ms do
    Application.get_env(:logflare, :spool, [])
    |> Keyword.get(:retry_delay_ms, @default_retry_delay_ms)
  end

  defp emit_storage_put_telemetry(format, bytes, result, upload_us) do
    :telemetry.execute(
      [:logflare, :backends, :spool, :storage, :put],
      %{count: 1, bytes: bytes, upload_duration: upload_us},
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
