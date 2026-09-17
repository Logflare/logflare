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
  """

  @behaviour DurableBuffer.Backend

  require Logger

  alias DurableBuffer.WAL
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
    case do_commit(state, IO.iodata_to_binary(batch), 0) do
      {:ok, state} ->
        Health.report_recovery!(:upload)
        {:ok, state}

      {:error, reason, state} ->
        Health.report_failure!(:upload)
        {:error, reason, state}
    end
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
    {payloads, _valid_bytes, _rest} = WAL.decode_all(body)
    msg = Jason.encode!(%{file_key: file_key, event_count: length(payloads)})

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
