defmodule Logflare.Backends.Adaptor.S3TablesAdaptor.Pipeline do
  @moduledoc """
  Pipeline for `S3TablesAdaptor`.

  Scaffolding only: batches are drained without being written to S3 Tables.
  """

  alias Broadway.Message
  alias Logflare.Backends.BufferProducer

  @producer_concurrency 1
  @processor_concurrency 5

  # batch events based on a maximum message count or byte length
  @max_batch_size 10_000
  @max_batch_length 8_000_000

  @doc false
  def child_spec(arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]}
    }
  end

  @doc false
  @spec start_link(Keyword.t()) ::
          {:ok, pid()} | :ignore | {:error, {:already_started, pid()} | term()}
  def start_link(args) when is_list(args) do
    with pipeline_name <- Keyword.fetch!(args, :pipeline_name),
         source_id <- Keyword.fetch!(args, :source_id),
         backend_id <- Keyword.fetch!(args, :backend_id),
         batch_timeout <- Keyword.fetch!(args, :batch_timeout) do
      Broadway.start_link(__MODULE__,
        name: pipeline_name,
        hibernate_after: 5_000,
        spawn_opt: [
          fullsweep_after: 10
        ],
        producer: [
          module: {BufferProducer, [source_id: source_id, backend_id: backend_id]},
          transformer: {__MODULE__, :transform, []},
          concurrency: @producer_concurrency
        ],
        processors: [
          default: [concurrency: @processor_concurrency, min_demand: 1]
        ],
        batchers: [
          s3_tables: [
            concurrency: 1,
            batch_size: batch_size_splitter(),
            max_demand: @max_batch_size,
            batch_timeout: batch_timeout
          ]
        ],
        context: %{source_id: source_id, backend_id: backend_id}
      )
    end
  end

  def handle_message(_processor_name, message, _adaptor_state) do
    Message.put_batcher(message, :s3_tables)
  end

  def handle_batch(:s3_tables, messages, _batch_info, _context) do
    # TODO: Arrow IPC encode the batch and write it via Native.append_batch/2
    messages
  end

  def transform(event, _opts) do
    %Message{
      data: event,
      acknowledger: {__MODULE__, :ack_id, :ack_data}
    }
  end

  def ack(_ack_ref, _successful, _failed) do
    # TODO: re-queue failed
  end

  # splits batch sizes based on message body size OR message count, whichever limit is reached first
  # https://hexdocs.pm/broadway/Broadway.html#start_link/2
  @spec batch_size_splitter() :: {tuple(), (any(), tuple() -> {:emit | :cont, tuple()})}
  defp batch_size_splitter do
    {
      {@max_batch_size, @max_batch_length},
      fn
        # reach max count, emit
        _message, {1, _len} ->
          {:emit, {@max_batch_size, @max_batch_length}}

        # check content length
        message, {count, len} ->
          length = message_size(message.data.body)

          if len - length <= 0 do
            # below max batch count, but reach max batch length
            {:emit, {@max_batch_size, @max_batch_length}}
          else
            # below max batch count, below max batch length
            {:cont, {count - 1, len - length}}
          end
      end
    }
  end

  @spec message_size(any()) :: non_neg_integer()
  defp message_size(data) do
    :erlang.external_size(data)
  end
end
