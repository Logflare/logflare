defmodule Logflare.Backends.Spool.ConsumerPipeline.QueueProducer do
  @moduledoc false

  use GenStage

  require Logger

  alias Logflare.Backends.Spool.MemoryMonitor

  @poll_interval 1_000
  @throttle_interval 100
  @min_empty_backoff_ms 100
  @max_empty_backoff_ms 1_000
  # How soon to retry once max_in_flight capacity frees up, instead of
  # waiting out the full @poll_interval.
  @min_in_flight_retry_ms 100
  # Process-dictionary key for this producer's in-flight :atomics ref — read
  # by ConsumerPipeline.transform/2, which Broadway runs in this same
  # process (per Broadway.Topology.ProducerStage), so no cross-process
  # registry is needed to hand the ref to the Acknowledger via ack_data.
  @in_flight_key :spool_queue_producer_in_flight_ref

  @doc "Returns this producer's in-flight ref — must be called from within the producer's own process."
  @spec get_in_flight_ref() :: :atomics.atomics_ref() | nil
  def get_in_flight_ref, do: Process.get(@in_flight_key)

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
    in_flight_ref = :atomics.new(1, signed: true)

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
      poll_timer: nil,
      poll_backoff_ms: @min_empty_backoff_ms,
      # source_ids already sent to MemoryMonitor.register_source/1 — sent
      # once per producer lifetime, never again.
      registered_sources: MapSet.new(),
      # Caps how many lines can be emitted to Broadway and not yet acked, so a
      # slow destination backend can't let this producer keep draining the
      # queue into an unbounded batcher backlog.
      in_flight_ref: in_flight_ref,
      max_in_flight: Keyword.get(opts, :max_in_flight, :infinity)
    }

    Process.put(@in_flight_key, in_flight_ref)

    {:producer, schedule_poll(state, 0)}
  end

  # Only forces an immediate poll when something already resolved is sitting
  # in memory and just needs acting on: buffered lines (emit them now), or a
  # completed prefetch not yet transferred into current (transfer-and-emit).
  # Otherwise there's nothing new to do — :running means the prefetch-result
  # handler will kick a poll when it lands, and nil means the fetch/backoff
  # loop is already driving itself forward on its own schedule.
  @impl GenStage
  def handle_demand(demand, state) do
    new_state = %{state | demand: state.demand + demand}

    {events, state} =
      cond do
        buffered?(new_state) and not over_limit?() ->
          {events, emitted_state} = emit_from_buffer(new_state)

          emitted_state =
            if capped_by_in_flight?(emitted_state),
              do: schedule_poll(emitted_state, @min_in_flight_retry_ms),
              else: emitted_state

          {events, emitted_state}

        match?({:ready, _}, new_state.prefetch) ->
          {[], schedule_poll(new_state, 0)}

        true ->
          {[], new_state}
      end

    {:noreply, events, state}
  end

  # The sole place that actually loads/emits, and the sole owner of the
  # periodic side of the poll loop: always reschedules itself at the end, on
  # every branch, so the loop can never permanently stop even if neither
  # handle_demand/2 nor :prefetch_result ever kicks it early.
  @impl GenStage
  def handle_info(:poll, state) do
    idle? = state.demand <= 0 or over_limit?()

    {events, new_state} =
      if idle? do
        {[], state}
      else
        state
        |> maybe_ack_exhausted()
        |> maybe_load_next()
        |> maybe_start_prefetch()
        |> emit_from_buffer()
      end

    {:noreply, events, schedule_poll(new_state, @poll_interval)}
  end

  # Records the result and, if demand is waiting with nothing buffered, kicks
  # a poll rather than waiting for the periodic loop — a real "something just
  # became available" signal (the background prefetch Task just completed),
  # not a guess. Real data or an error means the queue is active: reset the
  # empty-backoff and react immediately (delay 0). An empty result grows the
  # backoff (capped at @max_empty_backoff_ms) and uses it as the delay instead,
  # so a genuinely idle queue is polled less aggressively over time without
  # ever blocking this process — the fetch itself always runs in the Task
  # started by maybe_start_prefetch/1, never inline here.
  @impl GenStage
  def handle_info({:prefetch_result, result}, state) do
    {poll_backoff_ms, delay} =
      case result do
        :empty -> {min(state.poll_backoff_ms * 2, @max_empty_backoff_ms), state.poll_backoff_ms}
        _ -> {@min_empty_backoff_ms, 0}
      end

    new_state = %{state | prefetch: {:ready, result}, poll_backoff_ms: poll_backoff_ms}

    if state.demand > 0 and not buffered?(state) do
      {:noreply, [], schedule_poll(new_state, delay)}
    else
      {:noreply, [], new_state}
    end
  end

  # Sole function allowed to touch poll_timer / Process.send_after/cancel_timer
  # for the :poll message. Cancels whatever's pending before setting the next
  # one — defensive, since init/1, handle_demand/2, and handle_info/2 all call
  # this. Centralizes throttle enforcement: whatever delay a caller asks for,
  # if the system is currently over its memory/consumer limit, the next check
  # is always pushed out to @throttle_interval instead, so no call site needs
  # its own throttle-awareness beyond deciding whether to act right now.
  defp schedule_poll(state, delay) do
    effective_delay =
      cond do
        over_limit?() -> @throttle_interval
        capped_by_in_flight?(state) -> @min_in_flight_retry_ms
        true -> delay
      end

    if state.poll_timer, do: Process.cancel_timer(state.poll_timer)

    %{state | poll_timer: Process.send_after(self(), :poll, effective_delay)}
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

  # Prefetch still in flight, or not started yet — nothing to transfer.
  # handle_info(:prefetch_result) will send :poll once a Task lands; if
  # prefetch is nil, maybe_start_prefetch/1 (running right after this in the
  # same pipe) starts one. Fetching never happens inline in this process.
  defp maybe_load_next(%{current: nil} = state), do: state

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

  # Starts a background Task to fetch the next message/file whenever nothing
  # is already in flight — regardless of whether a file is currently being
  # streamed (current: %{}) or not (current: nil, e.g. cold start or an idle
  # queue). This is the only place safe_fetch_next runs; it never runs inline
  # in this process, so a slow or long-polling queue_mod.receive call (SQS/
  # PubSub) can never block handle_demand/2, handle_info/2, or :sys introspection.
  defp maybe_start_prefetch(%{prefetch: nil} = state) do
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

  defp emit_from_buffer(%{current: nil} = state), do: {[], state}
  defp emit_from_buffer(%{current: %{lines: []}} = state), do: {[], state}
  defp emit_from_buffer(%{demand: 0} = state), do: {[], state}

  defp emit_from_buffer(state) do
    to_take = min(state.demand, available_in_flight(state))
    {to_emit, remaining} = Enum.split(state.current.lines, to_take)

    if to_emit != [], do: :atomics.add(state.in_flight_ref, 1, length(to_emit))

    new_state = %{
      state
      | demand: state.demand - length(to_emit),
        current: %{state.current | lines: remaining}
    }

    {to_emit, new_state}
  end

  # A generous safety valve mirroring BufferProducer's capped_fetch_amount/2, not
  # a fine-grained flow-control knob — caps how many lines this producer will
  # hand to Broadway once too much already-emitted work is sitting unacked,
  # e.g. stuck deep in the batcher's own buffering while a destination backend
  # is slow. Should never engage during healthy operation.
  defp available_in_flight(%{max_in_flight: :infinity}), do: :infinity

  defp available_in_flight(%{in_flight_ref: ref, max_in_flight: max_in_flight}) do
    max(max_in_flight - :atomics.get(ref, 1), 0)
  end

  # True when there are lines buffered and demand waiting for them, but no
  # in-flight capacity to emit into right now — the specific condition that
  # warrants a fast retry instead of waiting out the normal poll cadence.
  defp capped_by_in_flight?(state) do
    buffered?(state) and state.demand > 0 and available_in_flight(state) == 0
  end

  # Wraps do_fetch_next so an unexpected exception always yields a normal
  # {:error, handle | nil, reason} result instead of propagating and crashing
  # the caller — always the unmonitored Task started by maybe_start_prefetch/1,
  # never the GenStage process itself. handle is nil when the crash happened
  # before a queue message was successfully retrieved.
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

          {:error, :not_found} ->
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

  # result is the raw return of the queue_mod.ack/nack call
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
