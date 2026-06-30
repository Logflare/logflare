defmodule Logflare.Backends.S3ConsumerPipeline.SqsProducer do
  @moduledoc false

  use GenStage

  require Logger

  @poll_interval 1_000
  @default_memory_limit_mb 4096
  @default_max_ets_mb 1024

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
    schedule_poll()

    {:producer,
     %{
       queue_url: queue_url,
       bucket: bucket,
       storage_mod: storage_mod,
       queue_mod: queue_mod,
       demand: 0,
       current: nil,
       # nil | :running | {:ready, fetch_result}
       # fetch_result = {:ok, handle, lines} | :empty | {:error, handle, reason}
       prefetch: nil
     }}
  end

  # Serve from the existing in-memory buffer only — never blocks on IO.
  # File loading happens in handle_info(:poll); prefetch runs concurrently in a Task.
  # When demand arrives with no buffer, kick an immediate poll rather than
  # waiting up to @poll_interval for the next tick.
  @impl GenStage
  def handle_demand(demand, state) do
    new_state = %{state | demand: state.demand + demand}

    if buffered?(new_state) and not over_limit?() do
      emit_from_buffer(new_state)
    else
      unless over_limit?(), do: send(self(), :poll)
      {:noreply, [], new_state}
    end
  end

  @impl GenStage
  def handle_info(:poll, state) do
    if state.demand <= 0 or over_limit?() do
      schedule_poll()
      {:noreply, [], state}
    else
      new_state =
        state
        |> maybe_ack_exhausted()
        |> maybe_load_next()

      new_state = maybe_start_prefetch(new_state)

      cond do
        new_state.current != nil ->
          # Emitting — demand will drive the next poll when this file exhausts
          :ok

        new_state.prefetch == :running ->
          # Prefetch download in flight; handle_info(:prefetch_result) will send :poll when ready
          :ok

        true ->
          # Queue was empty
          schedule_poll()
      end

      emit_from_buffer(new_state)
    end
  end

  @impl GenStage
  def handle_info({:prefetch_result, result}, state) do
    new_state = %{state | prefetch: {:ready, result}}

    # If demand is waiting and we have nothing buffered, kick a poll to load the prefetch now
    if state.demand > 0 and not buffered?(state) do
      send(self(), :poll)
    end

    {:noreply, [], new_state}
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

  # Prefetch landed but download failed — nack and fall through to empty
  defp maybe_load_next(%{current: nil, prefetch: {:ready, {:error, handle, reason}}} = state) do
    Logger.error("s3_consumer: prefetch failed: #{inspect(reason)}")
    state.queue_mod.nack(state.queue_url, handle)
    %{state | prefetch: nil}
  end

  # Prefetch still in flight — do nothing; handle_info(:prefetch_result) will send :poll
  defp maybe_load_next(%{current: nil, prefetch: :running} = state), do: state

  # No prefetch at all — blocking fetch (cold start or after queue-empty)
  defp maybe_load_next(%{current: nil, prefetch: nil} = state) do
    case do_fetch_next(state.queue_url, state.bucket, state.queue_mod, state.storage_mod) do
      {:ok, handle, lines} ->
        %{state | current: %{handle: handle, lines: lines}}

      :empty ->
        state

      {:error, handle, reason} ->
        Logger.error("s3_consumer: fetch failed: #{inspect(reason)}")
        state.queue_mod.nack(state.queue_url, handle)
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
        result = do_fetch_next(queue_url, bucket, queue_mod, storage_mod)
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
                Logger.warning("s3_consumer: file not found in storage, discarding stale queue entry: #{file_key}")
                queue_mod.ack(queue_url, handle)
                :empty

              {:error, reason} ->
                {:error, handle, reason}
            end

          _ ->
            Logger.warning("s3_consumer: queue message has no file_key, discarding")
            queue_mod.ack(queue_url, handle)
            :empty
        end

      {:ok, []} ->
        :empty

      {:error, reason} ->
        Logger.error("s3_consumer: queue receive failed: #{inspect(reason)}")
        :empty
    end
  end

  defp download_and_parse(bucket, file_key, storage_mod) do
    {download_us, download_result} = :timer.tc(fn -> storage_mod.get(bucket, file_key) end)
    dbg({:gcs_download_ms, Float.round(download_us / 1000, 1), file_key})

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

  defp over_limit? do
    s3_config = Application.get_env(:logflare, :s3_spool, [])
    total_limit_mb = Keyword.get(s3_config, :consumer_memory_limit_mb, @default_memory_limit_mb)
    ets_limit_mb = Keyword.get(s3_config, :consumer_max_ets_mb, @default_max_ets_mb)

    total = :erlang.memory(:total)
    ets = :erlang.memory(:ets)
    over = total > total_limit_mb * 1_048_576 or ets > ets_limit_mb * 1_048_576

    if over do
      total_mb = Float.round(total / 1_048_576, 1)
      ets_mb = Float.round(ets / 1_048_576, 1)
      dbg({"***************** s3_consumer THROTTLING *****************",
           total_mb: total_mb, total_limit_mb: total_limit_mb,
           ets_mb: ets_mb, ets_limit_mb: ets_limit_mb})
    end

    over
  end

  defp schedule_poll do
    Process.send_after(self(), :poll, @poll_interval)
  end
end
