defmodule Logflare.Backends.Spool.ConsumerPipeline.QueueProducer do
  @moduledoc false

  use GenStage

  require Logger

  alias Logflare.Backends.Spool.MemoryMonitor

  @poll_interval 1_000
  @throttle_interval 100

  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  @impl GenStage
  def init(opts) do
    queue_url = Keyword.fetch!(opts, :queue_url)
    bucket = Keyword.fetch!(opts, :bucket)
    storage_mod = Keyword.fetch!(opts, :storage_mod)
    queue_mod = Keyword.fetch!(opts, :queue_mod)

    state = %{
      queue_url: queue_url,
      bucket: bucket,
      storage_mod: storage_mod,
      queue_mod: queue_mod,
      demand: 0,
      current: nil,
      # nil | :running | {:ready, fetch_result}
      # fetch_result = {:ok, handle, lines} | :empty | {:error, handle, reason}
      prefetch: nil,
      # Process.send_after/3 timer ref for the one outstanding :poll trigger,
      # or nil if none is pending. Deduplicates concurrent handle_demand calls
      # (one per processor, see consumer_concurrency) so they can't each spawn
      # their own poll chain. Delay-0 sends (maybe_send_poll) go through
      # send_after too, purely to get a ref back — Process.read_timer/1 on
      # this field should always return a valid remaining time (or `false`
      # only in the brief window between firing and this field being cleared);
      # a stuck non-nil poll_timer that read_timer reports as `false` is a bug.
      poll_timer: nil,
      # source_ids already sent to MemoryMonitor.register_source/1 — sent
      # once per producer lifetime, never again. A hot source can appear in
      # every file this producer loads; without this we'd cast on every
      # single one instead of exactly once.
      registered_sources: MapSet.new()
    }

    # Immediate, not scheduled: no reason to wait @poll_interval for the very
    # first check.
    {:producer, maybe_send_poll(state)}
  end

  # Serve from the existing in-memory buffer only — never blocks on IO.
  # File loading happens in handle_info(:poll); prefetch runs concurrently in a Task.
  # When demand arrives with no buffer, kick an immediate poll rather than
  # waiting up to @poll_interval for the next tick. Under memory throttling we
  # still must schedule a fallback poll — never silently drop the only trigger
  # that would ever re-check state, or the producer freezes permanently even
  # after memory drops back under the limit.
  @impl GenStage
  def handle_demand(demand, state) do
    new_state = %{state | demand: state.demand + demand}
    throttled = over_limit?()

    cond do
      buffered?(new_state) and not throttled ->
        emit_from_buffer(new_state)

      throttled ->
        {:noreply, [], maybe_schedule_poll(new_state, @throttle_interval)}

      true ->
        {:noreply, [], maybe_send_poll(new_state)}
    end
  end

  @impl GenStage
  def handle_info(:poll, state) do
    state = %{state | poll_timer: nil}

    if state.demand <= 0 or over_limit?() do
      {:noreply, [], maybe_schedule_poll(state, @poll_interval)}
    else
      new_state =
        state
        |> maybe_ack_exhausted()
        |> maybe_load_next()

      new_state = maybe_start_prefetch(new_state)

      new_state =
        cond do
          new_state.current != nil ->
            # Emitting — demand will drive the next poll when this file exhausts
            new_state

          new_state.prefetch == :running ->
            # Prefetch download in flight; handle_info(:prefetch_result) will send :poll when ready
            new_state

          true ->
            # Queue was empty
            maybe_schedule_poll(new_state, @poll_interval)
        end

      emit_from_buffer(new_state)
    end
  end

  @impl GenStage
  def handle_info({:prefetch_result, result}, state) do
    new_state = %{state | prefetch: {:ready, result}}

    # If demand is waiting and we have nothing buffered, kick a poll to load the prefetch now
    new_state =
      if state.demand > 0 and not buffered?(state) do
        maybe_send_poll(new_state)
      else
        new_state
      end

    {:noreply, [], new_state}
  end

  defp maybe_schedule_poll(%{poll_timer: ref} = state, _interval) when not is_nil(ref), do: state

  defp maybe_schedule_poll(state, interval) do
    %{state | poll_timer: Process.send_after(self(), :poll, interval)}
  end

  defp maybe_send_poll(%{poll_timer: ref} = state) when not is_nil(ref), do: state

  defp maybe_send_poll(state) do
    %{state | poll_timer: Process.send_after(self(), :poll, 0)}
  end

  defp buffered?(%{current: nil}), do: false
  defp buffered?(%{current: %{lines: []}}), do: false
  defp buffered?(_), do: true

  defp maybe_ack_exhausted(%{current: %{lines: [], handle: handle}} = state) do
    ack_and_notify(state.queue_mod, state.queue_url, handle, :buffer_exhausted)
    %{state | current: nil}
  end

  defp maybe_ack_exhausted(state), do: state

  # Prefetch landed — use it immediately with zero download wait
  defp maybe_load_next(%{current: nil, prefetch: {:ready, {:ok, handle, lines}}} = state) do
    state = register_sources(state, lines)
    %{state | current: %{handle: handle, lines: lines}, prefetch: nil}
  end

  # Prefetch landed but queue was empty
  defp maybe_load_next(%{current: nil, prefetch: {:ready, :empty}} = state) do
    %{state | prefetch: nil}
  end

  # Prefetch landed but download failed — nack and fall through to empty.
  # handle may be nil if the prefetch task crashed before receiving a message.
  defp maybe_load_next(%{current: nil, prefetch: {:ready, {:error, handle, reason}}} = state) do
    if handle do
      # Routine, telemetry-covered failure (storage/network errors) — logging
      # every occurrence at :error would spam production under sustained
      # outages, so this is debug-only; the nack telemetry count is the
      # production-facing signal.
      Logger.debug("spool_consumer: prefetch failed: #{inspect(reason)}")
      nack_and_notify(state.queue_mod, state.queue_url, handle, :prefetch_failed)
    else
      # No handle means the crash happened before a queue message was even
      # retrieved — an unexpected internal error, not routine, worth a real log.
      Logger.error(
        "spool_consumer: prefetch crashed before receiving a message: #{inspect(reason)}"
      )
    end

    %{state | prefetch: nil}
  end

  # Prefetch still in flight — do nothing; handle_info(:prefetch_result) will send :poll
  defp maybe_load_next(%{current: nil, prefetch: :running} = state), do: state

  # No prefetch at all — blocking fetch (cold start or after queue-empty).
  # Wrapped so a bad message (unexpected exception) degrades to a logged skip
  # instead of crashing the whole producer and repeatedly re-fetching the same
  # poison message on every supervisor restart.
  defp maybe_load_next(%{current: nil, prefetch: nil} = state) do
    case safe_fetch_next(state.queue_url, state.bucket, state.queue_mod, state.storage_mod) do
      {:ok, handle, lines} ->
        state = register_sources(state, lines)
        %{state | current: %{handle: handle, lines: lines}}

      :empty ->
        state

      {:error, handle, reason} ->
        if handle do
          Logger.debug("spool_consumer: fetch failed: #{inspect(reason)}")
          nack_and_notify(state.queue_mod, state.queue_url, handle, :fetch_failed)
        else
          Logger.error(
            "spool_consumer: fetch crashed before receiving a message: #{inspect(reason)}"
          )
        end

        state
    end
  end

  defp maybe_load_next(state), do: state

  # Lets MemoryMonitor know these sources are currently flowing through the
  # spool consumer, so its refresh cycle checks their destination ingest
  # buffers for backlog (see over_limit?/0). Only casts for sources this
  # producer hasn't already sent — sent once per producer lifetime, never
  # again, since MemoryMonitor keeps a registered source watched permanently.
  defp register_sources(state, lines) do
    to_register =
      lines
      |> Enum.map(&record_source_id/1)
      |> Enum.reject(&is_nil/1)
      |> Enum.uniq()
      |> Enum.reject(&MapSet.member?(state.registered_sources, &1))

    Enum.each(to_register, &MemoryMonitor.register_source/1)

    registered_sources =
      Enum.reduce(to_register, state.registered_sources, &MapSet.put(&2, &1))

    %{state | registered_sources: registered_sources}
  end

  defp record_source_id(%{source_id: id}), do: id
  defp record_source_id(%{"source_id" => id}), do: id
  defp record_source_id(_), do: nil

  # Start a background Task to fetch the next file while we stream the current one.
  # Only when we have a current file and no prefetch already running.
  defp maybe_start_prefetch(%{prefetch: nil, current: %{}} = state) do
    if over_limit?() do
      state
    else
      parent = self()
      queue_url = state.queue_url
      bucket = state.bucket
      queue_mod = state.queue_mod
      storage_mod = state.storage_mod

      Task.start(fn ->
        # A crash here must still deliver a {:prefetch_result, _} message —
        # otherwise state.prefetch is stuck at :running forever (maybe_start_prefetch
        # refuses to start a new one, and nothing else will ever unstick it).
        result = safe_fetch_next(queue_url, bucket, queue_mod, storage_mod)
        send(parent, {:prefetch_result, result})
      end)

      %{state | prefetch: :running}
    end
  end

  defp maybe_start_prefetch(state), do: state

  defp emit_from_buffer(%{current: nil} = state), do: {:noreply, [], state}
  defp emit_from_buffer(%{current: %{lines: []}} = state), do: {:noreply, [], state}
  defp emit_from_buffer(%{demand: 0} = state), do: {:noreply, [], state}

  defp emit_from_buffer(state) do
    {to_emit, remaining} = Enum.split(state.current.lines, state.demand)

    new_state = %{
      state
      | demand: state.demand - length(to_emit),
        current: %{state.current | lines: remaining}
    }

    {:noreply, to_emit, new_state}
  end

  # Wraps do_fetch_next so an unexpected exception always yields a normal
  # {:error, handle | nil, reason} result instead of propagating and crashing
  # the caller (the GenStage process itself for the blocking path, or the
  # unmonitored Task for the prefetch path). handle is nil when the crash
  # happened before a queue message was successfully retrieved.
  defp safe_fetch_next(queue_url, bucket, queue_mod, storage_mod) do
    do_fetch_next(queue_url, bucket, queue_mod, storage_mod)
  rescue
    e -> {:error, nil, e}
  catch
    kind, reason -> {:error, nil, {kind, reason}}
  end

  defp do_fetch_next(queue_url, bucket, queue_mod, storage_mod) do
    result = queue_mod.receive(queue_url, max_number_of_messages: 1)

    :telemetry.execute(
      [:logflare, :backends, :spool, :queue, :receive],
      %{count: if(match?({:ok, _}, result), do: length(elem(result, 1)), else: 0)},
      %{result: if(match?({:ok, _}, result), do: :ok, else: :error)}
    )

    case result do
      {:ok, [%{id: handle, body: body}]} ->
        handle_received_message(handle, body, bucket, queue_url, queue_mod, storage_mod)

      {:ok, []} ->
        :empty

      {:error, reason} ->
        # Already covered by the [:queue, :receive] telemetry emitted above
        # with result: :error — debug-only to avoid duplicating that signal
        # as log spam under sustained queue issues.
        Logger.debug("spool_consumer: queue receive failed: #{inspect(reason)}")
        :empty
    end
  end

  defp handle_received_message(handle, body, bucket, queue_url, queue_mod, storage_mod) do
    case Jason.decode(body) do
      {:ok, %{"file_key" => file_key}} when is_binary(file_key) ->
        case download_and_parse(bucket, file_key, storage_mod) do
          {:ok, lines} ->
            {:ok, handle, lines}

          {:error, %Tesla.Env{status: 404}} ->
            # Already covered by the [:queue, :ack] telemetry (reason: :stale_file)
            # below — debug-only so a batch of stale entries doesn't spam prod.
            Logger.debug(
              "spool_consumer: file not found in storage, discarding stale queue entry: #{file_key}"
            )

            ack_and_notify(queue_mod, queue_url, handle, :stale_file)
            :empty

          {:error, {:decode_failed, exception}} ->
            # Unlike a transient storage error, corrupt/malformed spool content
            # will never succeed on retry — nacking it would poison the queue
            # with an infinite crash loop (see safe_fetch_next). Ack (drop) it
            # instead, same as a stale file, but stay loud since this indicates
            # real data corruption or a producer/consumer format mismatch.
            Logger.error(
              "spool_consumer: failed to decode spool file contents, discarding #{file_key}: #{Exception.format(:error, exception)}"
            )

            ack_and_notify(queue_mod, queue_url, handle, :decode_error)
            :empty

          {:error, reason} ->
            {:error, handle, reason}
        end

      _ ->
        Logger.debug("spool_consumer: queue message has no file_key, discarding")
        ack_and_notify(queue_mod, queue_url, handle, :no_file_key)
        :empty
    end
  end

  defp download_and_parse(bucket, file_key, storage_mod) do
    download_result = storage_mod.get(bucket, file_key)

    result =
      case download_result do
        {:ok, raw} -> decode_content(file_key, raw)
        {:error, reason} -> {:error, reason}
      end

    :telemetry.execute(
      [:logflare, :backends, :spool, :storage, :get],
      %{
        count: 1,
        bytes:
          if(match?({:ok, _}, download_result), do: byte_size(elem(download_result, 1)), else: 0),
        line_count: if(match?({:ok, _}, result), do: length(elem(result, 1)), else: 0)
      },
      %{result: if(match?({:ok, _}, result), do: :ok, else: :error)}
    )

    result
  end

  # Decompression and parsing are both capable of raising on truncated or
  # otherwise corrupt content (:zlib.gunzip/1 and :erlang.binary_to_term/2
  # both crash rather than returning an error tuple) — caught here, close to
  # the queue handle, so the caller can ack (drop) the poison message instead
  # of losing the handle to safe_fetch_next's outer rescue and retrying forever.
  defp decode_content(file_key, raw) do
    content = if String.ends_with?(file_key, ".gz"), do: :zlib.gunzip(raw), else: raw
    parse_content(file_key, content)
  rescue
    e -> {:error, {:decode_failed, e}}
  catch
    kind, reason -> {:error, {:decode_failed, %RuntimeError{message: inspect({kind, reason})}}}
  end

  defp parse_content(file_key, content) do
    base = String.replace_suffix(file_key, ".gz", "")

    if String.ends_with?(base, ".etf") do
      # No `:safe` — this content is produced by our own producer pipeline
      # with a small, fixed set of atom keys/values (id, source_id, body,
      # event_type, ingested_at, :log/:metric/:trace), never from untrusted
      # external input. `:safe` would additionally require every one of those
      # atoms to already exist in *this* node's atom table, which depends on
      # incidental module-load order (e.g. a freshly booted consumer-only node
      # that hasn't yet loaded `Logflare.LogEvent` won't have `:event_type`
      # interned) and fails every file until something else happens to load it.
      {:ok, :erlang.binary_to_term(content)}
    else
      lines =
        content
        |> String.split("\n", trim: true)
        |> Enum.flat_map(&decode_json_line/1)

      {:ok, lines}
    end
  end

  defp decode_json_line(line) do
    case Jason.decode(line) do
      {:ok, map} -> [map]
      {:error, _} -> []
    end
  end

  # Deliberately well below BufferLimiter's hardcoded 0.85 global threshold
  # (lib/logflare_web/controllers/plugs/buffer_limiter.ex) — the gap absorbs
  # the lag between "stop starting new fetches" and already-in-flight
  # downloads/decodes actually landing in memory, so the spool self-throttles
  # before it can ever contribute to a global 429 for unrelated sources.
  # Shared with the spool producer's early-flush decision via MemoryMonitor.
  #
  # consumer_throttled?/0 covers a different failure mode: a destination
  # source's own ingest buffer is backed up (e.g. downstream write pipeline
  # can't keep up), regardless of node memory pressure. Since this already
  # gates handle_info(:poll)/handle_demand/2 (see maybe_ack_exhausted/1,
  # maybe_load_next/1, emit_from_buffer/1), the current file's queue message
  # simply stops draining — and thus never gets acked — until the backlog
  # clears, instead of piling more events into an already-overflowing queue.
  defp over_limit? do
    MemoryMonitor.throttled?() or MemoryMonitor.consumer_throttled?()
  end

  # result is the raw return of the queue_mod.ack/nack call — previously
  # fire-and-forget, so a failure to actually delete/requeue the message with
  # SQS/PubSub was silently dropped. Normalized to :ok | :error to keep
  # cardinality low (no per-source or raw-reason tagging).
  defp emit_ack_telemetry(reason, result) do
    :telemetry.execute([:logflare, :backends, :spool, :queue, :ack], %{count: 1}, %{
      reason: reason,
      result: normalize_result(result)
    })
  end

  defp emit_nack_telemetry(reason, result) do
    :telemetry.execute([:logflare, :backends, :spool, :queue, :nack], %{count: 1}, %{
      reason: reason,
      result: normalize_result(result)
    })
  end

  defp normalize_result(:ok), do: :ok
  defp normalize_result(_), do: :error

  defp ack_and_notify(queue_mod, queue_url, handle, reason) do
    result = queue_mod.ack(queue_url, handle)
    emit_ack_telemetry(reason, result)
    result
  end

  defp nack_and_notify(queue_mod, queue_url, handle, reason) do
    result = queue_mod.nack(queue_url, handle)
    emit_nack_telemetry(reason, result)
    result
  end
end
