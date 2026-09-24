defmodule Logflare.Backends.Spool.ConsumerPipeline.QueueProducer do
  @moduledoc """
  GenStage producer for `ConsumerPipeline` — pulls queue messages (SQS or
  Pub/Sub, via `queue_mod`) pointing at spool files in `bucket`, downloads
  and splits each into segments (via `storage_mod`), and emits them to
  Broadway on demand.

  ## Polling and prefetch

  Fetching a queue message and downloading its file always runs in a
  background `Task` (`maybe_start_prefetch/1`), never inline, so a slow or
  long-polling `queue_mod.receive/2` call can't block this process. A
  periodic `:poll` message drives fetch → buffer → emit forward, polling
  sooner than the normal cadence when demand or a landed prefetch is
  waiting.

  ## Empty-queue backoff

  `poll_backoff_ms` doubles (capped at `@max_backoff`) after an empty
  prefetch result, and resets to `@min_backoff` the moment the queue has
  something again.

  ## Throttling

  `schedule_poll/2` is the sole place that arms the `:poll` timer, forcing
  a shorter delay whenever `over_limit?/0` (memory pressure or a backed-up
  destination) or `capped_by_in_flight?/1` is true. Fetching and emitting
  both pause entirely while `over_limit?/0` holds.

  ## max_in_flight

  Emitting to Broadway is capped by a byte budget (`max_in_flight`),
  tracked via an `:atomics` counter incremented in `emit_from_buffer/1` and
  decremented by `ConsumerPipeline`'s Acknowledger once Broadway finishes
  with a segment.

  ## Draining

  Implements `Broadway.Producer.prepare_for_draining/1`: freezes
  `handle_demand/2` and the `:poll` loop, and nacks anything not yet fully
  handed off (including any in-flight prefetch) so it's redelivered rather
  than silently lost.

  ## Queue acking

  This producer never acks a queue message directly. It only registers each
  handle with `SpoolAck` the moment it's obtained (`maybe_load_next/1`) and
  attaches it to every segment it emits — `SpoolAck` performs the actual ack
  once every event that handle's segments decoded into has finished
  processing (see its moduledoc). This producer draining or exiting does not
  lose that count: `SpoolAck` is an independently supervised process.

  ## Spool file format

  A spool file is one or more length+CRC32-framed chunks, compressed once
  as a whole. `decode_content/2` decompresses and splits it into segments;
  parsing each segment's content happens later, in
  `ConsumerPipeline.handle_message/3`, so it runs with Broadway's
  processor concurrency. A file whose version tag is newer than this
  build recognizes is left for a node that understands it
  (`{:unsupported_version, _}`) rather than treated as corrupt.
  """

  @behaviour Broadway.Producer

  use GenStage

  require Logger

  alias Logflare.Backends.Spool.Encoder
  alias Logflare.Backends.Spool.MemoryMonitor
  alias Logflare.Backends.Spool.SpoolAck

  @throttle_interval 100
  @min_backoff 100
  @max_backoff 1_000
  # Process-dictionary key for this producer's in-flight :atomics ref —
  # read by ConsumerPipeline.transform/2, run in this same process.
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
      # fetch_result = {:ok, handle, segments} | :empty | {:error, handle, reason}
      prefetch: nil,
      poll_timer: nil,
      poll_backoff_ms: @min_backoff,
      # Caps how many bytes can be emitted to Broadway and not yet acked.
      in_flight_ref: in_flight_ref,
      max_in_flight: Keyword.get(opts, :max_in_flight, :infinity),
      # Set by prepare_for_draining/1 — stops new fetches while letting
      # anything already in flight drain out and ack normally.
      draining: false
    }

    Process.put(@in_flight_key, in_flight_ref)

    {:producer, schedule_poll(state, 0)}
  end

  @impl Broadway.Producer
  def prepare_for_draining(state) do
    if handle = current_handle(state.current) do
      nack_and_notify(state.queue_mod, state.queue_url, handle, :draining)
    end

    if handle = prefetch_handle(state.prefetch) do
      nack_and_notify(state.queue_mod, state.queue_url, handle, :draining)
    end

    {:noreply, [], %{state | draining: true, demand: 0, current: nil, prefetch: nil}}
  end

  # A fully-drained current has nothing left to nack — its segments are
  # already emitted, and its fate from here is entirely SpoolAck's.
  defp current_handle(%{segments: [], handle: _}), do: nil
  defp current_handle(%{handle: handle}), do: handle
  defp current_handle(nil), do: nil

  defp prefetch_handle({:ready, {:ok, handle, _segments}}), do: handle
  defp prefetch_handle({:ready, {:error, handle, _reason}}), do: handle
  defp prefetch_handle(_), do: nil

  @impl GenStage
  def handle_demand(_demand, %{draining: true} = state), do: {:noreply, [], state}

  def handle_demand(demand, state) do
    new_state = %{state | demand: state.demand + demand}

    {events, state} =
      cond do
        buffered?(new_state) and not over_limit?() ->
          {events, emitted_state} = emit_from_buffer(new_state)

          emitted_state =
            if capped_by_in_flight?(emitted_state),
              do: schedule_poll(emitted_state, @min_backoff),
              else: emitted_state

          {events, emitted_state}

        match?({:ready, _}, new_state.prefetch) ->
          {[], schedule_poll(new_state, 0)}

        true ->
          {[], new_state}
      end

    {:noreply, events, state}
  end

  # Always reschedules itself at the end, on every branch, so the loop can
  # never permanently stop.
  @impl GenStage
  def handle_info(:poll, %{draining: true} = state), do: {:noreply, [], state}

  def handle_info(:poll, state) do
    idle? = state.demand <= 0 or over_limit?()

    {duration, {events, new_state}} =
      :timer.tc(fn ->
        if idle? do
          {[], state}
        else
          state
          |> maybe_clear_exhausted()
          |> maybe_load_next()
          |> maybe_start_prefetch()
          |> emit_from_buffer()
        end
      end)

    :telemetry.execute(
      [:logflare, :backends, :spool, :consumer, :poll],
      %{duration: duration},
      %{idle: idle?}
    )

    {:noreply, events, schedule_poll(new_state, @max_backoff)}
  end

  @impl GenStage
  def handle_info({:prefetch_result, result}, %{draining: true} = state) do
    case result do
      {:ok, handle, _segments} ->
        nack_and_notify(state.queue_mod, state.queue_url, handle, :draining)

      {:error, handle, _reason} when not is_nil(handle) ->
        nack_and_notify(state.queue_mod, state.queue_url, handle, :draining)

      _ ->
        :ok
    end

    {:noreply, [], state}
  end

  def handle_info({:prefetch_result, result}, state) do
    {poll_backoff_ms, delay} =
      case result do
        :empty -> {min(state.poll_backoff_ms * 2, @max_backoff), state.poll_backoff_ms}
        _ -> {@min_backoff, 0}
      end

    :telemetry.execute(
      [:logflare, :backends, :spool, :queue, :poll_backoff],
      %{backoff_ms: poll_backoff_ms},
      %{}
    )

    new_state = %{state | prefetch: {:ready, result}, poll_backoff_ms: poll_backoff_ms}

    if state.demand > 0 and not buffered?(state) do
      {:noreply, [], schedule_poll(new_state, delay)}
    else
      {:noreply, [], new_state}
    end
  end

  # Sole place allowed to touch poll_timer. Centralizes throttle enforcement
  # over whatever delay a caller asks for.
  defp schedule_poll(state, delay) do
    effective_delay =
      cond do
        over_limit?() -> @throttle_interval
        capped_by_in_flight?(state) -> @min_backoff
        true -> delay
      end

    if state.poll_timer, do: Process.cancel_timer(state.poll_timer)

    %{state | poll_timer: Process.send_after(self(), :poll, effective_delay)}
  end

  defp buffered?(%{current: nil}), do: false
  defp buffered?(%{current: %{segments: []}}), do: false
  defp buffered?(_), do: true

  # Unblocks maybe_load_next/1 once a file's segments are all emitted —
  # SpoolAck (not this producer) owns acking it from here.
  defp maybe_clear_exhausted(%{current: %{segments: []}} = state),
    do: %{state | current: nil}

  defp maybe_clear_exhausted(state), do: state

  defp maybe_load_next(%{current: nil, prefetch: {:ready, {:ok, handle, segments}}} = state) do
    SpoolAck.register(handle, state.queue_mod, state.queue_url)
    %{state | current: %{handle: handle, segments: segments}, prefetch: nil}
  end

  defp maybe_load_next(%{current: nil, prefetch: {:ready, :empty}} = state) do
    %{state | prefetch: nil}
  end

  # handle is nil if the prefetch task crashed before receiving a message.
  defp maybe_load_next(%{current: nil, prefetch: {:ready, {:error, handle, reason}}} = state) do
    if handle do
      Logger.debug("spool_consumer: prefetch failed: #{inspect(reason)}")
      nack_and_notify(state.queue_mod, state.queue_url, handle, :prefetch_failed)
    else
      Logger.error(
        "spool_consumer: prefetch crashed before receiving a message: #{inspect(reason)}"
      )
    end

    %{state | prefetch: nil}
  end

  defp maybe_load_next(%{current: nil} = state), do: state

  defp maybe_load_next(state), do: state

  # The only place safe_fetch_next runs — always in a background Task,
  # never inline in this process.
  defp maybe_start_prefetch(%{prefetch: nil} = state) do
    if over_limit?() do
      state
    else
      parent = self()
      queue_url = state.queue_url
      bucket = state.bucket
      queue_mod = state.queue_mod
      storage_mod = state.storage_mod
      started_while_buffered? = buffered?(state)

      Task.start(fn ->
        run_prefetch(
          parent,
          queue_url,
          bucket,
          queue_mod,
          storage_mod,
          started_while_buffered?
        )
      end)

      %{state | prefetch: :running}
    end
  end

  defp maybe_start_prefetch(state), do: state

  # A crash here must still deliver a {:prefetch_result, _} message, or
  # state.prefetch is stuck at :running forever.
  defp run_prefetch(parent, queue_url, bucket, queue_mod, storage_mod, started_while_buffered?) do
    parent_ref = Process.monitor(parent)

    {duration, result} =
      :timer.tc(fn -> safe_fetch_next(queue_url, bucket, queue_mod, storage_mod) end)

    :telemetry.execute(
      [:logflare, :backends, :spool, :consumer, :prefetch],
      %{duration: duration},
      %{result: prefetch_result_tag(result), started_while_buffered: started_while_buffered?}
    )

    deliver_or_settle(result, parent, parent_ref, queue_mod, queue_url)
  end

  defp prefetch_result_tag({:ok, _handle, _segments}), do: :ok
  defp prefetch_result_tag(:empty), do: :empty
  defp prefetch_result_tag({:error, _handle, _reason}), do: :error

  # The producer can be killed mid-fetch. send/2 to an already-dead parent
  # is a silent no-op, so check the monitor first and settle the handle
  # directly here instead of leaving it stranded until the queue's
  # visibility timeout expires.
  defp deliver_or_settle(result, parent, parent_ref, queue_mod, queue_url) do
    receive do
      {:DOWN, ^parent_ref, :process, ^parent, _reason} ->
        settle_orphaned_result(result, queue_mod, queue_url)
    after
      0 ->
        send(parent, {:prefetch_result, result})
        Process.demonitor(parent_ref, [:flush])
    end
  end

  defp settle_orphaned_result({:ok, handle, _segments}, queue_mod, queue_url),
    do: nack_and_notify(queue_mod, queue_url, handle, :producer_gone)

  defp settle_orphaned_result({:error, handle, _reason}, queue_mod, queue_url)
       when not is_nil(handle),
       do: nack_and_notify(queue_mod, queue_url, handle, :producer_gone)

  defp settle_orphaned_result(_result, _queue_mod, _queue_url), do: :ok

  defp emit_from_buffer(%{current: nil} = state), do: {[], state}
  defp emit_from_buffer(%{current: %{segments: []}} = state), do: {[], state}
  defp emit_from_buffer(%{demand: 0} = state), do: {[], state}

  defp emit_from_buffer(state) do
    nothing_in_flight? = :atomics.get(state.in_flight_ref, 1) == 0

    count =
      take_count_within_budget(
        state.current.segments,
        state.demand,
        available_in_flight(state),
        nothing_in_flight?
      )

    {to_emit, remaining} = Enum.split(state.current.segments, count)
    bytes_emitted = Enum.sum(Enum.map(to_emit, &byte_size/1))

    if to_emit != [], do: :atomics.add(state.in_flight_ref, 1, bytes_emitted)

    handle = state.current.handle
    events = Enum.map(to_emit, &%{segment: &1, handle: handle})

    new_state = %{
      state
      | demand: state.demand - length(to_emit),
        current: %{state.current | segments: remaining}
    }

    {events, new_state}
  end

  # How many of the first `max_count` segments fit within `available_bytes`.
  # The first segment is exempt from the budget only when nothing is
  # currently in flight at all, so a segment bigger than the whole budget
  # can't permanently stall this producer.
  defp take_count_within_budget(segments, max_count, available_bytes, nothing_in_flight?) do
    segments
    |> Enum.take(max_count)
    |> Enum.reduce_while({0, 0}, fn segment, {count, bytes_used} ->
      size = byte_size(segment)
      exempt? = count == 0 and nothing_in_flight?

      if not exempt? and bytes_used + size > available_bytes do
        {:halt, {count, bytes_used}}
      else
        {:cont, {count + 1, bytes_used + size}}
      end
    end)
    |> elem(0)
  end

  # A generous safety valve mirroring BufferProducer's capped_fetch_amount/2, not
  # a fine-grained flow-control knob — caps how many bytes this producer will
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
    {duration, result} =
      :timer.tc(fn -> queue_mod.receive(queue_url, max_number_of_messages: 1) end)

    :telemetry.execute(
      [:logflare, :backends, :spool, :queue, :receive],
      %{
        count: if(match?({:ok, _}, result), do: length(elem(result, 1)), else: 0),
        duration: duration
      },
      %{result: if(match?({:ok, _}, result), do: :ok, else: :error)}
    )

    case result do
      {:ok, [%{id: handle, body: body}]} ->
        handle_received_message(handle, body, bucket, queue_url, queue_mod, storage_mod)

      {:ok, []} ->
        :empty

      {:error, reason} ->
        Logger.debug("spool_consumer: queue receive failed: #{inspect(reason)}")
        :empty
    end
  end

  defp handle_received_message(handle, body, bucket, queue_url, queue_mod, storage_mod) do
    case Jason.decode(body) do
      {:ok, %{"file_key" => file_key}} when is_binary(file_key) ->
        case download_and_parse(bucket, file_key, storage_mod) do
          {:ok, segments} ->
            {:ok, handle, segments}

          {:error, :not_found} ->
            Logger.debug(
              "spool_consumer: file not found in storage, discarding stale queue entry: #{file_key}"
            )

            ack_and_notify(queue_mod, queue_url, handle, :stale_file)
            :empty

          {:error, {:decode_failed, exception}} ->
            Logger.error(
              "spool_consumer: failed to decode spool file contents, discarding #{file_key}: #{Exception.format(:error, exception)}"
            )

            ack_and_notify(queue_mod, queue_url, handle, :decode_error)
            :empty

          {:error, {:unsupported_version, _version}} ->
            nack_and_notify(queue_mod, queue_url, handle, :unsupported_version)
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
    {duration, download_result} = :timer.tc(fn -> storage_mod.get(bucket, file_key) end)

    result =
      case download_result do
        {:ok, raw} -> decode_content(file_key, raw)
        {:error, reason} -> {:error, reason}
      end

    :telemetry.execute(
      [:logflare, :backends, :spool, :storage, :get],
      %{
        bytes:
          if(match?({:ok, _}, download_result), do: byte_size(elem(download_result, 1)), else: 0),
        segment_count: if(match?({:ok, _}, result), do: length(elem(result, 1)), else: 0),
        duration: duration
      },
      %{result: if(match?({:ok, _}, result), do: :ok, else: :error)}
    )

    result
  end

  defp decode_content(file_key, raw) do
    current = Encoder.current_version()

    case Encoder.file_key_version(file_key) do
      ^current ->
        {duration, decompressed} = :timer.tc(fn -> decompress_by_extension(raw, file_key) end)

        :telemetry.execute(
          [:logflare, :backends, :spool, :consumer, :decompress],
          %{duration: duration, bytes: byte_size(raw)},
          %{}
        )

        case Encoder.decode_segments(decompressed) do
          {[], _valid, _rest} -> {:error, {:decode_failed, :not_framed}}
          {segments, _valid, _rest} -> {:ok, segments}
        end

      version ->
        {:error, {:unsupported_version, version}}
    end
  rescue
    e -> {:error, {:decode_failed, e}}
  catch
    kind, reason -> {:error, {:decode_failed, %RuntimeError{message: inspect({kind, reason})}}}
  end

  defp decompress_by_extension(content, file_key) do
    if String.ends_with?(file_key, ".zst") do
      decompress_zstd!(content)
    else
      content
    end
  end

  # Normalizes :ezstd.decompress/1's {:error, _} return to a raise, so it's
  # caught by decode_content/2's rescue like every other decode failure.
  defp decompress_zstd!(raw) do
    case :ezstd.decompress(raw) do
      binary when is_binary(binary) -> binary
      {:error, reason} -> raise "zstd decompression failed: #{inspect(reason)}"
    end
  end

  defp over_limit? do
    MemoryMonitor.throttled?() or MemoryMonitor.consumer_throttled?()
  end

  # result is the raw return of the queue_mod.ack/nack call
  defp emit_ack_telemetry(reason, result) do
    :telemetry.execute([:logflare, :backends, :spool, :queue, :ack], %{}, %{
      reason: reason,
      result: normalize_result(result)
    })
  end

  defp emit_nack_telemetry(reason, result) do
    :telemetry.execute([:logflare, :backends, :spool, :queue, :nack], %{}, %{
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
