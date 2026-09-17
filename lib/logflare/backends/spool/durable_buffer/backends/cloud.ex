defmodule Logflare.Backends.Spool.DurableBuffer.Backends.Cloud do
  @moduledoc """
  `DurableBuffer.Backend` that uploads each group commit straight to
  GCS/S3 and publishes a queue notification, with no local disk step —
  spool's "mem" modes (sync and async) ride on `DurableBuffer.Partition`'s
  group-commit engine instead of our own.

  A commit is durable exactly when the upload+notify succeeds, so
  `DurableBuffer.append/3`'s blocking return already matches "mem sync"
  mode; `DurableBuffer.append_async/3` (no wait) matches "mem async".
  There is no local WAL, so `stream/2`/`truncate/2` have nothing to do.

  Implements the optional async commit contract (`commit_async/5` +
  `handle_message/2`) so `DurableBuffer.Partition.Committer` can pipeline
  uploads — up to `max_inflight_commits` concurrent GCS/queue round
  trips per partition — instead of one commit blocking the next behind
  it. `state` is never mutated by a commit either way, so handing a copy
  to a concurrent `Task` is safe. `commit/4` stays synchronous and is
  used unconditionally by `RotatingWal.Worker`, which calls it directly
  rather than through any `DurableBuffer.Partition`.
  """

  @behaviour DurableBuffer.Backend

  require Logger

  alias Logflare.Backends.Spool.Encoder
  alias Logflare.Backends.Spool.Health

  @default_max_commit_attempts 5
  @default_retry_delay_ms 100

  @impl true
  def init_config(opts) do
    %{
      bucket: Keyword.fetch!(opts, :bucket),
      storage_mod: Keyword.fetch!(opts, :storage_mod),
      queue_mod: Keyword.fetch!(opts, :queue_mod),
      queue_ref: Keyword.get(opts, :queue_ref),
      compress: Keyword.get(opts, :compress, true),
      max_commit_attempts: Keyword.get(opts, :max_commit_attempts, @default_max_commit_attempts),
      retry_delay_ms: Keyword.get(opts, :retry_delay_ms, @default_retry_delay_ms)
    }
  end

  @impl true
  def open(config, partition_index) do
    {:ok, %{config: config, partition_index: partition_index}}
  end

  @impl true
  def commit(state, batch, _byte_size, _span) do
    body = IO.iodata_to_binary(batch)
    emit_handle_batch_telemetry(body)

    case do_commit(state, body, 0) do
      {:ok, state} ->
        Health.report_recovery!(:upload)
        {:ok, state}

      {:error, reason, state} ->
        Health.report_failure!(:upload)
        {:error, reason, state}
    end
  end

  @impl true
  def commit_async(state, batch, _byte_size, _span, tag) do
    body = IO.iodata_to_binary(batch)
    emit_handle_batch_telemetry(body)

    parent = self()
    Task.start(fn -> send(parent, {:backend, {tag, safe_commit_result(state, body)}}) end)

    {:pending, state}
  end

  @impl true
  def handle_message({tag, result}, state) do
    case result do
      :ok -> Health.report_recovery!(:upload)
      {:error, _reason} -> Health.report_failure!(:upload)
    end

    {[{tag, result}], state}
  end

  # Runs in an unlinked Task, never the Committer — a crash here must
  # still resolve `tag`, or the Committer's in-flight credit for it is
  # never released and this partition eventually stalls (mirrors
  # QueueProducer.safe_fetch_next/4's same guarantee).
  defp safe_commit_result(state, body) do
    case do_commit(state, body, 0) do
      {:ok, _state} -> :ok
      {:error, reason, _state} -> {:error, reason}
    end
  rescue
    e -> {:error, e}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  # Mirrors the shared [:logflare, :backends, :pipeline, :handle_batch]
  # event every other ingest pipeline emits, so the same dashboards/alerts
  # apply — fired once per commit, before the upload is attempted, same as
  # the rest. batch_trigger is always nil: DurableBuffer.Partition doesn't
  # expose why this batch formed (size vs. dwell) to the backend.
  defp emit_handle_batch_telemetry(body) do
    :telemetry.execute(
      [:logflare, :backends, :pipeline, :handle_batch],
      %{batch_size: Encoder.count_events(body), batch_trigger: nil},
      %{backend_type: :spool_producer, batch_trigger: nil}
    )
  end

  defp do_commit(state, body, attempt) do
    case upload_and_notify(state, body) do
      :ok ->
        {:ok, state}

      {:error, reason} ->
        if attempt + 1 < state.config.max_commit_attempts do
          Process.sleep(state.config.retry_delay_ms)
          do_commit(state, body, attempt + 1)
        else
          {:error, reason, state}
        end
    end
  end

  defp upload_and_notify(state, body) do
    config = state.config
    file_key = Encoder.build_file_key(state.partition_index, config.compress)
    compressed_body = maybe_compress(body, config)

    with {:ok, _} <-
           config.storage_mod.put(
             config.bucket,
             file_key,
             compressed_body,
             Encoder.upload_headers(config.compress)
           ),
         :ok <- notify_queue(config, file_key, body) do
      :ok
    end
  end

  defp maybe_compress(body, %{compress: true}), do: Encoder.compress_binary(body)
  defp maybe_compress(body, %{compress: false}), do: body

  defp notify_queue(%{queue_ref: nil}, _file_key, _body), do: :ok

  defp notify_queue(config, file_key, body) do
    msg = Jason.encode!(%{file_key: file_key, event_count: Encoder.count_events(body)})

    case config.queue_mod.publish(config.queue_ref, msg) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def stream(_config, _partition_index) do
    raise "#{inspect(__MODULE__)} does not support stream/2 — consumption happens via " <>
            "the queue/storage fan-out (ConsumerPipeline), not by reading the buffer back."
  end

  @impl true
  def truncate(state, _next_offset) do
    # Nothing local ever accumulates past a commit — a commit only settles
    # once the upload+notify above already succeeded.
    {:ok, state}
  end

  @impl true
  def close(_state), do: :ok
end
