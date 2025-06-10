defmodule Logflare.Backends.Adaptor.S3Adaptor.Pipeline do
  @moduledoc """
  Pipeline for `S3Adaptor`

  This pipeline is responsible for taking log events from the
  source backend and inserting them into the configured S3 bucket.
  """

  alias Broadway.Message
  alias Logflare.Backends.Adaptor.S3Adaptor
  alias Logflare.Backends.BufferProducer

  @producer_concurrency 1
  @processor_concurrency 5
  @batch_size 10_000

  @doc false
  def child_spec(arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]}
    }
  end

  @doc false
  @spec start_link(S3Adaptor.t()) ::
          {:ok, pid()} | :ignore | {:error, {:already_started, pid()} | term()}
  def start_link(%S3Adaptor{} = adaptor_state) do
    Broadway.start_link(__MODULE__,
      name: adaptor_state.pipeline_name,
      producer: [
        module:
          {BufferProducer,
           [source_id: adaptor_state.source.id, backend_id: adaptor_state.backend.id]},
        transformer: {__MODULE__, :transform, []},
        concurrency: @producer_concurrency
      ],
      processors: [
        default: [concurrency: @processor_concurrency, min_demand: 1]
      ],
      batchers: [
        s3: [
          concurrency: 1,
          batch_size: @batch_size,
          batch_timeout: adaptor_state.config.batch_timeout
        ]
      ],
      context: adaptor_state
    )
  end

  # see the implementation for `Backends.via_source/2` for how tuples are used to identify child processes
  def process_name({:via, module, {registry, identifier}}, base_name) do
    new_identifier = Tuple.append(identifier, base_name)
    {:via, module, {registry, new_identifier}}
  end

  def handle_message(_processor_name, message, _adaptor_state) do
    Message.put_batcher(message, :s3)
  end

  def handle_batch(:s3, messages, _batch_info, %{source: source, backend: backend}) do
    events = for %{data: le} <- messages, do: le
    S3Adaptor.push_log_events_to_s3({source, backend}, events)
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
