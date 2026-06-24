defmodule Logflare.Backends.S3ConsumerPipeline do
  @moduledoc false

  use Broadway

  require Logger

  alias Broadway.Message
  alias Logflare.Backends
  alias Logflare.Backends.S3ConsumerPipeline.SqsProducer
  alias Logflare.Sources

  @behaviour Broadway.Acknowledger

  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(args) do
    {name, _args} = Keyword.pop!(args, :name)

    s3_config = Application.get_env(:logflare, :s3_spool, [])
    bucket = Keyword.fetch!(s3_config, :bucket)
    concurrency = Keyword.get(s3_config, :consumer_concurrency, 4)
    batch_size = Keyword.get(s3_config, :consumer_batch_size, 5_000)
    queue_name = Keyword.fetch!(s3_config, :queue_name)
    queue_url = resolve_queue_url!(queue_name)

    Broadway.start_link(__MODULE__,
      name: name,
      producer: [
        module: {SqsProducer, [queue_url: queue_url, bucket: bucket]},
        transformer: {__MODULE__, :transform, []}
      ],
      processors: [
        default: [concurrency: concurrency, max_demand: 10]
      ],
      batchers: [
        default: [
          batch_size: batch_size,
          batch_timeout: 1_000,
          concurrency: 1
        ]
      ]
    )
  end

  @spec transform(map(), keyword()) :: Message.t()
  def transform(line, _opts) do
    %Message{
      data: line,
      acknowledger: {__MODULE__, :noop, nil}
    }
  end

  # SQS acking is managed by the producer — individual message ack is a no-op.
  @impl Broadway.Acknowledger
  def ack(_ack_ref, _successful, failed) do
    if failed != [] do
      Logger.error("s3_consumer: #{length(failed)} messages failed during processing")
    end

    :ok
  end

  @impl Broadway
  def handle_message(_processor, %Message{} = message, _context) do
    message
  end

  @impl Broadway
  def handle_batch(_batcher, messages, _batch_info, _context) do
    messages
    |> Enum.map(& &1.data)
    |> Enum.group_by(& &1["source_id"])
    |> Enum.each(fn
      {nil, lines} ->
        Logger.warning("s3_consumer: #{length(lines)} events missing source_id, skipping")

      {source_id, lines} ->
        case Sources.get(source_id) do
          nil ->
            Logger.warning("s3_consumer: unknown source_id=#{source_id}, skipping #{length(lines)} events")

          source ->
            event_params = Enum.map(lines, & &1["body"])

            case Backends.dispatch_from_s3(event_params, source) do
              {:ok, _} -> :ok
              {:error, reason} -> Logger.error("s3_consumer: dispatch failed for source #{source_id}: #{inspect(reason)}")
            end
        end
    end)

    messages
  end

  defp resolve_queue_url!(queue_name) do
    case ExAws.SQS.get_queue_url(queue_name) |> ExAws.request() do
      {:ok, %{body: %{queue_url: url}}} ->
        url

      {:error, reason} ->
        raise "s3_consumer: failed to resolve SQS queue URL for #{queue_name}: #{inspect(reason)}"
    end
  end
end
