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
    {:producer, %{queue_url: queue_url, bucket: bucket, storage_mod: storage_mod, queue_mod: queue_mod, demand: 0, current: nil}}
  end

  # Serve from the existing in-memory buffer only — never blocks on IO.
  # File loading happens exclusively in handle_info(:poll).
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

      # If the queue was empty, back off to the periodic interval.
      # If we got a file, Broadway will send demand again immediately which
      # triggers handle_demand → send(self(), :poll) for the next file.
      if new_state.current == nil, do: schedule_poll()

      emit_from_buffer(new_state)
    end
  end

  defp buffered?(%{current: nil}), do: false
  defp buffered?(%{current: %{lines: []}}), do: false
  defp buffered?(_), do: true

  defp maybe_ack_exhausted(%{current: %{lines: [], handle: handle}} = state) do
    state.queue_mod.ack(state.queue_url, handle)
    %{state | current: nil}
  end

  defp maybe_ack_exhausted(state), do: state

  defp maybe_load_next(%{current: nil} = state) do
    case fetch_next(state.queue_url, state.bucket, state.queue_mod, state.storage_mod) do
      nil -> state
      current -> %{state | current: current}
    end
  end

  defp maybe_load_next(state), do: state

  defp emit_from_buffer(%{current: nil} = state), do: {:noreply, [], state}
  defp emit_from_buffer(%{current: %{lines: []}} = state), do: {:noreply, [], state}
  defp emit_from_buffer(%{demand: 0} = state), do: {:noreply, [], state}

  defp emit_from_buffer(state) do
    {to_emit, remaining} = Enum.split(state.current.lines, state.demand)

    new_state = %{state |
      demand: state.demand - length(to_emit),
      current: %{state.current | lines: remaining}
    }

    {:noreply, to_emit, new_state}
  end

  defp fetch_next(queue_url, bucket, queue_mod, storage_mod) do
    {queue_us, result} = :timer.tc(fn -> queue_mod.receive(queue_url, max_number_of_messages: 1) end)
    dbg({:queue_receive_ms, Float.round(queue_us / 1000, 1)})

    case result do
      {:ok, [%{id: handle, body: body}]} ->
        load_file(queue_url, bucket, handle, body, queue_mod, storage_mod)

      {:ok, []} ->
        nil

      {:error, reason} ->
        Logger.error("s3_consumer: queue receive failed: #{inspect(reason)}")
        nil
    end
  end

  defp load_file(queue_url, bucket, handle, body, queue_mod, storage_mod) do
    case Jason.decode(body) do
      {:ok, %{"file_key" => file_key}} when is_binary(file_key) ->
        case download_and_parse(bucket, file_key, storage_mod) do
          {:ok, lines} ->
            %{handle: handle, lines: lines}

          {:error, reason} ->
            Logger.error("s3_consumer: failed to download #{file_key}: #{inspect(reason)}")
            case queue_mod.nack(queue_url, handle) do
              :ok -> :ok
              {:error, nack_reason} -> Logger.error("s3_consumer: nack failed: #{inspect(nack_reason)}")
            end
            nil
        end

      _ ->
        Logger.warning("s3_consumer: queue message has no file_key, discarding")
        queue_mod.ack(queue_url, handle)
        nil
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

        dbg({:decompress_ms, Float.round(decompress_us / 1000, 1), :parse_ms, Float.round(parse_us / 1000, 1), :line_count, length(lines)})

        {:ok, lines}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_content(file_key, content) do
    base = file_key |> String.replace_suffix(".gz", "")

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
    total_limit = Keyword.get(s3_config, :consumer_memory_limit_mb, @default_memory_limit_mb)
    ets_limit = Keyword.get(s3_config, :consumer_max_ets_mb, @default_max_ets_mb)

    :erlang.memory(:total) > total_limit * 1_048_576 or
      :erlang.memory(:ets) > ets_limit * 1_048_576
  end

  defp schedule_poll do
    Process.send_after(self(), :poll, @poll_interval)
  end
end
