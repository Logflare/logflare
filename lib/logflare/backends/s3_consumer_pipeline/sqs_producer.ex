defmodule Logflare.Backends.S3ConsumerPipeline.SqsProducer do
  @moduledoc false

  use GenStage

  require Logger

  @poll_interval 1_000
  @default_memory_limit_mb 2048
  @default_max_ets_mb 512

  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  @impl GenStage
  def init(opts) do
    queue_url = Keyword.fetch!(opts, :queue_url)
    bucket = Keyword.fetch!(opts, :bucket)
    schedule_poll()
    {:producer, %{queue_url: queue_url, bucket: bucket, demand: 0, current: nil}}
  end

  # Serve from the existing in-memory buffer only — never blocks on IO.
  # File loading happens exclusively in handle_info(:poll).
  @impl GenStage
  def handle_demand(demand, state) do
    new_state = %{state | demand: state.demand + demand}

    if buffered?(new_state) and not over_limit?() do
      emit_from_buffer(new_state)
    else
      {:noreply, [], new_state}
    end
  end

  @impl GenStage
  def handle_info(:poll, state) do
    schedule_poll()

    if state.demand <= 0 or over_limit?() do
      {:noreply, [], state}
    else
      state
      |> maybe_ack_exhausted()
      |> maybe_load_next()
      |> emit_from_buffer()
    end
  end

  defp buffered?(%{current: nil}), do: false
  defp buffered?(%{current: %{lines: []}}), do: false
  defp buffered?(_), do: true

  defp maybe_ack_exhausted(%{current: %{lines: [], handle: handle}} = state) do
    ack_sqs(state.queue_url, handle)
    %{state | current: nil}
  end

  defp maybe_ack_exhausted(state), do: state

  defp maybe_load_next(%{current: nil} = state) do
    case fetch_next(state.queue_url, state.bucket) do
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

  defp fetch_next(queue_url, bucket) do
    case sqs_request(ExAws.SQS.receive_message(queue_url, max_number_of_messages: 1)) do
      {:ok, %{body: %{messages: [%{body: body, receipt_handle: handle}]}}} ->
        load_file(queue_url, bucket, handle, body)

      {:ok, %{body: %{messages: []}}} ->
        nil

      {:error, reason} ->
        Logger.error("s3_consumer: SQS receive failed: #{inspect(reason)}")
        nil
    end
  end

  defp load_file(queue_url, bucket, handle, body) do
    case Jason.decode(body) do
      {:ok, %{"file_key" => file_key}} when is_binary(file_key) ->
        case download_and_parse(bucket, file_key) do
          {:ok, lines} ->
            %{handle: handle, lines: lines}

          {:error, reason} ->
            Logger.error("s3_consumer: failed to download #{file_key}: #{inspect(reason)}")
            nack_sqs(queue_url, handle)
            nil
        end

      _ ->
        Logger.warning("s3_consumer: SQS message has no file_key, discarding")
        ack_sqs(queue_url, handle)
        nil
    end
  end

  defp download_and_parse(bucket, file_key) do
    case sqs_request(ExAws.S3.get_object(bucket, file_key)) do
      {:ok, %{body: raw}} ->
        content =
          if String.ends_with?(file_key, ".gz") do
            :zlib.gunzip(raw)
          else
            raw
          end

        lines =
          content
          |> String.split("\n", trim: true)
          |> Enum.flat_map(fn line ->
            case Jason.decode(line) do
              {:ok, map} -> [map]
              {:error, _} -> []
            end
          end)

        {:ok, lines}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp ack_sqs(queue_url, handle) do
    # ElasticMQ returns 200 with an empty body for DeleteMessage, which the XML
    # parser raises on. A 200 means the delete landed — ignore parse errors.
    # Real network failures let the visibility timeout expire and redeliver.
    sqs_request(ExAws.SQS.delete_message(queue_url, handle))
    :ok
  end

  defp nack_sqs(queue_url, handle) do
    case sqs_request(ExAws.SQS.change_message_visibility(queue_url, handle, 0)) do
      {:ok, _} -> :ok
      {:error, reason} -> Logger.error("s3_consumer: SQS nack failed: #{inspect(reason)}")
    end
  end

  defp sqs_request(operation) do
    try do
      ExAws.request(operation)
    rescue
      e -> {:error, Exception.message(e)}
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
