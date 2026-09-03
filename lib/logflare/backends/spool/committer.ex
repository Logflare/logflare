defmodule Logflare.Backends.Spool.Committer do
  @moduledoc """
  Performs the actual GCS/S3 PUT and Pub-Sub/SQS publish for a batch handed
  off by its paired `Partition`, then replies directly to every blocking
  caller in that batch — no `Broadway.Acknowledger` indirection (compare
  `ProducerPipeline.ack/3`). Segments arrive already compressed and framed
  (see `Logflare.Backends.Spool.Encoder`/`Framing`) — a commit is just
  concatenation plus I/O.

  Retries are scoped narrowly: only a `storage_mod.put/4` or
  `queue_mod.publish/2` failure is retriable (up to `max_retries`,
  mirroring `ProducerPipeline.maybe_requeue_failed/1`) — encoding/compression
  already succeeded, in the caller's own process, before an entry ever
  reached the Partition, so there's nothing else here that can fail.
  """

  use GenServer

  import Bitwise

  require Logger

  alias Logflare.Backends.Spool.Encoder
  alias Logflare.Backends.Spool.Partition

  @default_max_retries 0

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @spec commit(
          GenServer.server(),
          [Partition.entry()],
          non_neg_integer(),
          non_neg_integer(),
          atom()
        ) ::
          :ok
  def commit(committer, entries, total_bytes, total_count, trigger) do
    GenServer.cast(committer, {:commit, entries, total_bytes, total_count, trigger})
  end

  @impl GenServer
  def init(opts) do
    {:ok,
     %{
       partition: Keyword.fetch!(opts, :partition),
       index: Keyword.fetch!(opts, :index),
       bucket: Keyword.fetch!(opts, :bucket),
       storage_mod: Keyword.fetch!(opts, :storage_mod),
       queue_mod: Keyword.fetch!(opts, :queue_mod),
       queue_ref: Keyword.fetch!(opts, :queue_ref),
       format: Keyword.fetch!(opts, :format),
       compress: Keyword.fetch!(opts, :compress),
       compression_algorithm: Keyword.fetch!(opts, :compression_algorithm)
     }}
  end

  @impl GenServer
  def handle_cast({:commit, entries, _total_bytes, total_count, trigger}, state) do
    :telemetry.execute(
      [:logflare, :backends, :pipeline, :handle_batch],
      %{batch_size: total_count, batch_trigger: trigger},
      %{backend_type: :spool_producer, batch_trigger: trigger}
    )

    file_key = file_key(state)

    {concat_us, body} =
      :timer.tc(fn -> entries |> Enum.map(&elem(&1, 1)) |> IO.iodata_to_binary() end)

    {result, upload_us} = commit_io(state, file_key, body, total_count)

    format_tag = Encoder.format_tag(state.format, state.compress, state.compression_algorithm)
    emit_storage_put_telemetry(format_tag, byte_size(body), result, concat_us, upload_us)

    case result do
      {:ok, _file_key} ->
        emit_batch_result(:ok, nil, total_count)
        Logger.debug("spool_committer: wrote #{total_count} events to spool", key: file_key)
        Enum.each(entries, fn {from, _seg, _bs, _ec, _r} -> reply(from, :ok) end)

      {stage, {:error, reason}} ->
        emit_batch_result(:error, stage, total_count)

        Logger.error("spool_committer: #{stage} failed key=#{file_key} error=#{inspect(reason)}")

        maybe_requeue_failed(state, entries, reason)
    end

    send(state.partition, :commit_done)
    {:noreply, state}
  end

  defp file_key(state) do
    ext = Encoder.file_extension(state.format, state.compress, state.compression_algorithm)
    "#{state.index}/#{generate_uuidv7()}.#{ext}"
  end

  # Returns the commit result alongside the GCS/S3 upload's own duration in
  # microseconds — separate from notify_queue/3's own duration measurement —
  # so logflare.backends.spool.storage.put.upload_duration reflects only the
  # storage_mod.put call, not the queue publish that follows it.
  @spec commit_io(map(), String.t(), binary(), non_neg_integer()) ::
          {{:ok, String.t()} | {:upload | :notify, {:error, term()}}, non_neg_integer()}
  defp commit_io(state, file_key, body, total_count) do
    if disable_commit_io?() do
      {{:ok, file_key}, 0}
    else
      {upload_us, upload_result} =
        :timer.tc(fn -> state.storage_mod.put(state.bucket, file_key, body, headers(state)) end)

      result =
        with {:upload, {:ok, _}} <- {:upload, upload_result},
             {:notify, :ok} <- {:notify, notify_queue(state, file_key, total_count)} do
          {:ok, file_key}
        end

      {result, upload_us}
    end
  end

  defp headers(state) do
    base = %{"content-type" => Encoder.content_type(state.format)}

    headers =
      case Encoder.content_encoding(state.compress, state.compression_algorithm) do
        nil -> base
        encoding -> Map.put(base, "content-encoding", encoding)
      end

    [headers: headers]
  end

  defp notify_queue(%{queue_ref: nil}, _file_key, _count), do: :ok

  defp notify_queue(state, file_key, count) do
    body = Jason.encode!(%{file_key: file_key, event_count: count})
    {publish_us, result} = :timer.tc(fn -> state.queue_mod.publish(state.queue_ref, body) end)

    :telemetry.execute(
      [:logflare, :backends, :spool, :queue, :publish],
      %{count: 1, duration: publish_us},
      %{result: if(result == :ok, do: :ok, else: :error), mode: :notify}
    )

    case result do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error("spool_committer: queue notify failed for #{file_key}: #{inspect(reason)}")

        {:error, reason}
    end
  end

  # Every entry in a failed commit is retried or given up on as a whole,
  # same as ProducerPipeline.maybe_requeue_failed/1 — a chunk was already
  # atomically admitted as one unit, so it's never split into "some retry,
  # some don't". A :notify failure hits this same path even though the file
  # is already durably written: re-uploading under a fresh key on retry is
  # simpler than tracking "durably written but never notified" as a distinct
  # state, and the orphaned file is cleaned up by the bucket's lifecycle policy.
  defp maybe_requeue_failed(state, entries, reason) do
    max_retries = spool_max_retries()

    {retriable, exhausted} =
      Enum.split_with(entries, fn {_from, _seg, _bs, _ec, retries} -> retries < max_retries end)

    if exhausted != [] do
      Logger.warning(
        "spool_committer: dropping #{length(exhausted)} chunks: exhausted #{max_retries} retries"
      )

      Enum.each(exhausted, fn {from, _seg, _bs, _ec, _r} -> reply(from, {:error, reason}) end)
    end

    if retriable != [] do
      Logger.warning("spool_committer: requeuing #{length(retriable)} failed chunks for retry")

      bumped =
        Enum.map(retriable, fn {from, seg, bs, ec, retries} ->
          {from, seg, bs, ec, retries + 1}
        end)

      Partition.requeue(state.partition, bumped)
    end
  end

  defp reply(nil, _result), do: :ok
  defp reply(from, result), do: GenServer.reply(from, result)

  defp spool_max_retries do
    Application.get_env(:logflare, :spool, [])
    |> Keyword.get(:max_retries, @default_max_retries)
  end

  # TEMP: perf isolation switch for the CPU regression investigation on
  # feat/spool_block_till_write. When true, skips the actual
  # storage_mod.put/4 (GCS/S3) upload and queue_mod.publish/2 (Pub-Sub/SQS)
  # call, and just replies :ok to every blocking caller as if the commit
  # succeeded — the concat/framing work above still runs. Lets us measure
  # committer overhead with the network I/O out of the picture. Remove once
  # the bottleneck is identified.
  @spec disable_commit_io?() :: boolean()
  defp disable_commit_io? do
    Application.get_env(:logflare, :spool, []) |> Keyword.get(:disable_commit_io, false)
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
