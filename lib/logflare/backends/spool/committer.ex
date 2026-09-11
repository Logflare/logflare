defmodule Logflare.Backends.Spool.Committer do
  @moduledoc """
  Uploads one commit's body to GCS/S3 and notifies Pub-Sub/SQS, retrying on
  failure, then reports the outcome back to `acknowledger` (always a
  `Logflare.Backends.Spool.Partition`) as `{:commit_success, context}` or
  `{:commit_failed, context, reason}` — `context` is opaque here, never
  inspected, just handed back unchanged (in practice, a `Partition`-local
  tag it uses to look up what it actually needs to react — see its
  moduledoc).

  Doesn't know what a "file" or a "batch" is, either: `body_thunk` is
  whatever the caller needs to produce the bytes — read a sealed WAL file,
  concatenate already-in-memory segments, anything (see
  `Logflare.Backends.Spool.Buffer`) — called once per attempt inside this
  module's own spawned `Task`, so however slow that is never blocks the
  caller's own process.

  A failed upload/notify attempt is retried up to `max_commit_attempts/0`
  times (config, default 5), logging each failure, then gives up — retrying
  forever isn't worth the liability of an unbounded loop. What "giving up"
  costs depends entirely on the buffer: a WAL commit's sealed file is never
  deleted except on success, so it's still on disk for the next restart's
  recovery scan; the in-memory buffer has no such backup, so its blocked
  callers (if any — see `Partition.append/5`'s `wait_until_committed` opt)
  just get `{:error, reason}`.

  A body that can't even be *produced* (the thunk itself fails — an
  unreadable file, say), or whose frames fail CRC validation (see
  `Logflare.Backends.Spool.Framing`), is a different failure class, though:
  unlike an upload failing, retrying either can never succeed — so both log
  loudly, emit telemetry, and give up immediately rather than spending any
  retry attempts on it. Checking frames here, before ever uploading, catches
  disk corruption (or, for a group-commit batch, a bug in how it was
  assembled) at the source instead of only downstream when a consumer
  eventually fails to decode the file.
  """

  require Logger

  alias Logflare.Backends.Spool.Encoder
  alias Logflare.Backends.Spool.Framing

  @default_retry_delay_ms 100
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
  Spawns an unlinked `Task` that calls `body_thunk` to produce the bytes to
  commit, retrying up to `max_commit_attempts/0` times on an upload/notify
  failure (re-invoking `body_thunk` each attempt), then reports the result
  back to `acknowledger` as `{:commit_success, context}` or
  `{:commit_failed, context, reason}`. Returns the task's pid so the caller
  can monitor it — a crashing commit is the caller's concern, not this
  module's.
  """
  @spec commit_async(
          pid(),
          (-> {:ok, binary()} | {:error, term()}),
          non_neg_integer(),
          atom(),
          term(),
          config()
        ) :: {:ok, pid()}
  def commit_async(acknowledger, body_thunk, total_count, trigger, context, config) do
    Task.start(fn ->
      case do_commit(body_thunk, total_count, trigger, config, 0) do
        {:ok, _file_key} -> send(acknowledger, {:commit_success, context})
        {:error, reason} -> send(acknowledger, {:commit_failed, context, reason})
      end
    end)
  end

  defp do_commit(body_thunk, total_count, trigger, config, attempt) do
    with {:ok, body} <- body_thunk.(),
         :ok <- validate_body(body) do
      commit_body(body_thunk, body, total_count, trigger, config, attempt)
    else
      {:error, reason} ->
        :telemetry.execute(
          [:logflare, :backends, :spool, :committer, :read_error],
          %{},
          %{reason: reason}
        )

        Logger.error(
          "spool_committer: could not obtain a valid body, dropping #{total_count} events: " <>
            "#{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  # Refuses only if nothing in the body is salvageable at all.
  defp validate_body(body) do
    case Framing.decode_segments(body) do
      {:ok, _segments} ->
        :ok

      {:error, :corrupt, []} ->
        {:error, :corrupt}

      {:error, :corrupt, _decoded} ->
        Logger.warning(
          "spool_committer: body has at least one corrupt segment, committing the " <>
            "salvageable segments anyway"
        )

        :ok

      {:error, :not_framed} ->
        {:error, :not_framed}
    end
  end

  defp commit_body(body_thunk, body, total_count, trigger, config, attempt) do
    case upload_and_notify(body, config, total_count, trigger) do
      {:ok, file_key} ->
        Logger.debug("spool_committer: wrote #{total_count} events to spool", key: file_key)
        {:ok, file_key}

      {:error, {stage, reason}} ->
        max_attempts = max_commit_attempts()

        Logger.error(
          "spool_committer: #{stage} failed attempt=#{attempt + 1}/#{max_attempts} " <>
            "error=#{inspect(reason)}"
        )

        if attempt + 1 < max_attempts do
          Process.sleep(retry_delay_ms())
          do_commit(body_thunk, total_count, trigger, config, attempt + 1)
        else
          {:error, reason}
        end
    end
  end

  @doc """
  Compresses (if configured) and uploads `body`, then notifies the queue for
  one commit's worth of work, emitting the shared
  `handle_batch`/`storage.put`/`producer.batch` telemetry — every commit's
  upload goes through here, whether `body` came from one sealed WAL file or
  a concatenated group-commit batch, so this is the one place that
  telemetry needs to be emitted at all. `body` arrives raw and is
  compressed here, once, as a whole, right before upload.
  """
  @spec upload_and_notify(binary(), config(), non_neg_integer(), atom()) ::
          {:ok, file_key :: String.t()} | {:error, {atom(), term()}}
  def upload_and_notify(body, config, total_count, trigger) do
    :telemetry.execute(
      [:logflare, :backends, :pipeline, :handle_batch],
      %{batch_size: total_count, batch_trigger: trigger},
      %{backend_type: :spool_producer, batch_trigger: trigger}
    )

    file_key = file_key(config)
    compressed_body = maybe_compress(body, config)

    {upload_us, upload_result} =
      :timer.tc(fn ->
        config.storage_mod.put(config.bucket, file_key, compressed_body, headers(config))
      end)

    result =
      with {:upload, {:ok, _}} <- {:upload, upload_result},
           {:notify, :ok} <- {:notify, notify_queue(config, file_key, total_count)} do
        {:ok, file_key}
      else
        {stage, {:error, reason}} -> {:error, {stage, reason}}
      end

    format_tag = Encoder.format_tag(config.format, config.compress, config.compression_algorithm)
    emit_storage_put_telemetry(format_tag, byte_size(compressed_body), result, upload_us)

    case result do
      {:ok, file_key} ->
        emit_batch_result(:ok, nil, total_count)
        {:ok, file_key}

      {:error, {stage, reason}} ->
        emit_batch_result(:error, stage, total_count)
        {:error, {stage, reason}}
    end
  end

  defp maybe_compress(body, %{compress: true, compression_algorithm: algorithm}),
    do: Encoder.compress_binary(algorithm, body)

  defp maybe_compress(body, %{compress: false}), do: body

  defp file_key(config) do
    Encoder.build_file_key(
      config.index,
      config.format,
      config.compress,
      config.compression_algorithm
    )
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
      %{},
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
      %{bytes: bytes, upload_duration: upload_us},
      %{format: format, result: if(match?({:ok, _}, result), do: :ok, else: :error)}
    )
  end

  defp emit_batch_result(result, stage, batch_size) do
    :telemetry.execute(
      [:logflare, :backends, :spool, :producer, :batch],
      %{batch_size: batch_size},
      %{result: result, stage: stage}
    )
  end
end
