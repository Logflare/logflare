defmodule Logflare.Backends.Adaptor.S3Adaptor.Pipeline do
  @moduledoc """
  Pipeline for `S3Adaptor`

  This pipeline is responsible for taking log events from the
  source backend and inserting them into the configured S3 bucket.
  """

  alias Broadway.Message
  alias Logflare.Backends.Adaptor.S3Adaptor
  alias Logflare.Backends.BufferProducer
  alias Logflare.Backends.Pipeline.BatchSplitter
  alias Logflare.Utils

  @producer_concurrency 1
  @processor_concurrency 5

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
          s3: [
            concurrency: 1,
            batch_size: BatchSplitter.build(),
            max_demand: BatchSplitter.max_batch_size(),
            batch_timeout: batch_timeout
          ]
        ],
        context: %{source_id: source_id, backend_id: backend_id}
      )
    end
  end

  # see the implementation for `Backends.via_source/2` for how tuples are used to identify child processes
  def process_name({:via, module, {registry, identifier}}, base_name) do
    new_identifier = Utils.append_to_tuple(identifier, base_name)
    {:via, module, {registry, new_identifier}}
  end

  def handle_message(_processor_name, message, _adaptor_state) do
    Message.put_batcher(message, :s3)
  end

  def handle_batch(:s3, messages, _batch_info, %{source_id: source_id, backend_id: backend_id}) do
    events = for %{data: le} <- messages, do: le
    S3Adaptor.push_log_events_to_s3({source_id, backend_id}, events)
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
end
