defmodule Logflare.Backends.Adaptor.HttpBased.Pipeline do
  @moduledoc """
  Common Broadway pipeline for HTTP-based adaptors sending log batches using Tesla client
  """

  use Broadway
  alias Broadway.Message
  alias Logflare.Backends
  alias Logflare.Backends.BufferProducer
  alias Logflare.Utils

  def start_link(source, backend, client) do
    Broadway.start_link(__MODULE__,
      name: Backends.via_source(source.id, __MODULE__, backend.id),
      hibernate_after: 5_000,
      spawn_opt: [
        fullsweep_after: 100
      ],
      producer: [
        module:
          {BufferProducer,
           [
             backend_id: backend.id,
             source_id: source.id
           ]},
        transformer: {__MODULE__, :transform, []},
        concurrency: 1
      ],
      processors: [
        default: [concurrency: 3, min_demand: 1]
      ],
      batchers: [
        http: [concurrency: 6, batch_size: 250]
      ],
      context: %{
        source_id: source.id,
        backend_id: backend.id,
        source_token: source.token,
        backend_token: backend.token,
        client: client
      }
    )
  end

  # see the implementation for Backends.via_source/2 for how tuples are used to identify child processes
  def process_name({:via, module, {registry, identifier}}, base_name) do
    new_identifier = Utils.append_to_tuple(identifier, base_name)
    {:via, module, {registry, new_identifier}}
  end

  def handle_message(_processor_name, message, _context) do
    message
    |> Message.put_batcher(:http)
  end

  def handle_batch(:http, messages, batch_info, context) do
    :telemetry.execute(
      [:logflare, :backends, :pipeline, :handle_batch],
      %{batch_size: batch_info.size, batch_trigger: batch_info.trigger},
      %{
        backend_type: :http_based
      }
    )

    %{metadata: backend_metadata} = backend = Backends.Cache.get_backend(context.backend_id)
    config = Backends.Adaptor.get_backend_config(backend)

    events = for %{data: le} <- messages, do: le

    backend_meta =
      for {k, v} <- backend_metadata || %{}, into: %{} do
        {"backend.#{k}", v}
      end

    metadata =
      %{
        "source_id" => context[:source_id],
        "source_uuid" => context[:source_token],
        "backend_id" => context[:backend_id],
        "backend_uuid" => context[:backend_token]
      }
      |> Map.merge(backend_meta)

    context.client.send_logs(config, events, metadata)

    messages
  end

  # Broadway transformer for custom producer
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
