defmodule Logflare.Backends.Adaptor.PostgresAdaptor.Pipeline do
  @moduledoc """
  Pipeline for PostgresAdaptor

  This pipeline is responsible for taking log events from the source backend and inserting them into the configured database.
  """
  use Broadway

  alias Broadway.Message
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Backends.BufferProducer

  @spec start_link(PostgresAdaptor.t()) :: {:ok, pid()}
  def start_link(adaptor_state) do
    Broadway.start_link(__MODULE__,
      name: adaptor_state.pipeline_name,
      producer: [
        module:
          {BufferProducer,
           [source_id: adaptor_state.source.id, backend_id: adaptor_state.backend.id]},
        transformer: {__MODULE__, :transform, []},
        concurrency: 1
      ],
      processors: [
        default: [concurrency: 5, min_demand: 1]
      ],
      batchers: [
        pg: [concurrency: 5, batch_size: 350]
      ],
      context: adaptor_state
    )
  end

  # see the implementation for Backends.via_source/2 for how tuples are used to identify child processes
  def process_name({:via, module, {registry, identifier}}, base_name) do
    new_identifier = Tuple.insert_at(identifier, tuple_size(identifier), base_name)
    {:via, module, {registry, new_identifier}}
  end

  def handle_message(_processor_name, message, _adaptor_state) do
    Message.put_batcher(message, :pg)
  end

  def handle_batch(:pg, messages, _batch_info, %{source: source, backend: backend}) do
    events = for %{data: le} <- messages, do: le
    PostgresAdaptor.insert_log_events(source, backend, events)
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
