defmodule Logflare.Backends.Spool.ConsumerPipeline.QueueProducer do
  @moduledoc false

  use GenStage

  require Logger

  alias Logflare.Backends.Spool.MemoryMonitor

  @poll_interval 1_000

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
      poll_timer: nil
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
        {:noreply, [], maybe_schedule_poll(new_state)}

      true ->
        {:noreply, [], maybe_send_poll(new_state)}
    end
  end

  @impl GenStage
  def handle_info(:poll, state) do
    state = %{state | poll_timer: nil}

    if state.demand <= 0 or over_limit?() do
      {:noreply, [], maybe_schedule_poll(state)}
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
            maybe_schedule_poll(new_state)
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

  defp maybe_schedule_poll(%{poll_timer: ref} = state) when not is_nil(ref), do: state

  defp maybe_schedule_poll(state) do
    %{state | poll_timer: Process.send_after(self(), :poll, @poll_interval)}
  end

  defp maybe_send_poll(%{poll_timer: ref} = state) when not is_nil(ref), do: state

  defp maybe_send_poll(state) do
    %{state | poll_timer: Process.send_after(self(), :poll, 0)}
  end

  defp buffered?(%{current: nil}), do: false
  defp buffered?(%{current: %{lines: []}}), do: false
  defp buffered?(_), do: true

  defp maybe_ack_exhausted(%{current: %{lines: [], handle: handle}} = state) do
    state.queue_mod.ack(state.queue_url, handle)
    %{state | current: nil}
  end

  defp maybe_ack_exhausted(state), do: state

  # Prefetch landed — use it immediately with zero download wait
  defp maybe_load_next(%{current: nil, prefetch: {:ready, {:ok, handle, lines}}} = state) do
    dbg({:prefetch_hit, length(lines)})
    %{state | current: %{handle: handle, lines: lines}, prefetch: nil}
  end

  # Prefetch landed but queue was empty
  defp maybe_load_next(%{current: nil, prefetch: {:ready, :empty}} = state) do
    %{state | prefetch: nil}
  end

  # Prefetch landed but download failed — nack and fall through to empty.
  # handle may be nil if the prefetch task crashed before receiving a message.
  defp maybe_load_next(%{current: nil, prefetch: {:ready, {:error, handle, reason}}} = state) do
    Logger.error("spool_consumer: prefetch failed: #{inspect(reason)}")
    if handle, do: state.queue_mod.nack(state.queue_url, handle)
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
        %{state | current: %{handle: handle, lines: lines}}

      :empty ->
        state

      {:error, handle, reason} ->
        Logger.error("spool_consumer: fetch failed: #{inspect(reason)}")
        if handle, do: state.queue_mod.nack(state.queue_url, handle)
        state
    end
  end

  defp maybe_load_next(state), do: state

  # Start a background Task to fetch the next file while we stream the current one.
  # Only when we have a current file and no prefetch already running.
  defp maybe_start_prefetch(%{prefetch: nil, current: %{}} = state) do
    if not over_limit?() do
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
    else
      state
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
    {queue_us, result} = :timer.tc(fn -> queue_mod.receive(queue_url, max_number_of_messages: 1) end)
    dbg({:queue_receive_ms, Float.round(queue_us / 1000, 1)})

    case result do
      {:ok, [%{id: handle, body: body}]} ->
        case Jason.decode(body) do
          {:ok, %{"file_key" => file_key}} when is_binary(file_key) ->
            case download_and_parse(bucket, file_key, storage_mod) do
              {:ok, lines} ->
                {:ok, handle, lines}

              {:error, %Tesla.Env{status: 404}} ->
                Logger.warning("spool_consumer: file not found in storage, discarding stale queue entry: #{file_key}")
                queue_mod.ack(queue_url, handle)
                :empty

              {:error, reason} ->
                {:error, handle, reason}
            end

          _ ->
            Logger.warning("spool_consumer: queue message has no file_key, discarding")
            queue_mod.ack(queue_url, handle)
            :empty
        end

      {:ok, []} ->
        :empty

      {:error, reason} ->
        Logger.error("spool_consumer: queue receive failed: #{inspect(reason)}")
        :empty
    end
  end

  defp download_and_parse(bucket, file_key, storage_mod) do
    {download_us, download_result} = :timer.tc(fn -> storage_mod.get(bucket, file_key) end)
    dbg({:storage_download_ms, Float.round(download_us / 1000, 1), file_key})

    case download_result do
      {:ok, raw} ->
        {decompress_us, content} =
          :timer.tc(fn ->
            if String.ends_with?(file_key, ".gz"), do: :zlib.gunzip(raw), else: raw
          end)

        {parse_us, lines} =
          :timer.tc(fn -> parse_content(file_key, content) end)

        dbg({:decompress_ms, Float.round(decompress_us / 1000, 1), :parse_ms,
         Float.round(parse_us / 1000, 1), :line_count, length(lines)})

        {:ok, lines}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_content(file_key, content) do
    base = String.replace_suffix(file_key, ".gz", "")

    if String.ends_with?(base, ".etf") do
      :erlang.binary_to_term(content, [:safe])
    else
      content
      |> String.split("\n", trim: true)
      |> Enum.flat_map(fn line ->
        case Jason.decode(line) do
          {:ok, map} -> [map]
          {:error, _} -> []
        end
      end)
    end
  end

  # Deliberately well below BufferLimiter's hardcoded 0.85 global threshold
  # (lib/logflare_web/controllers/plugs/buffer_limiter.ex) — the gap absorbs
  # the lag between "stop starting new fetches" and already-in-flight
  # downloads/decodes actually landing in memory, so the spool self-throttles
  # before it can ever contribute to a global 429 for unrelated sources.
  # Shared with the spool producer's early-flush decision via MemoryMonitor.
  defp over_limit? do
    stats = MemoryMonitor.stats()

    if stats.throttled? do
      dbg({"***************** spool_consumer THROTTLING *****************",
           total_percent: Float.round(stats.total_percent * 100, 1), total_limit_percent: stats.total_limit_percent * 100,
           ets_percent: Float.round(stats.ets_percent * 100, 1), ets_limit_percent: stats.ets_limit_percent * 100})
    end

    stats.throttled?
  end
end
