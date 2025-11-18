defmodule Logflare.Backends.Adaptor.ClickhouseAdaptor.Pipeline do
  @moduledoc """
  Pipeline for `ClickhouseAdaptor`

  This pipeline is responsible for taking log events from the
  source backend and inserting them into the configured database.
  """

  require OpenTelemetry.Tracer

  alias Broadway.Message
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.ClickhouseAdaptor
  alias Logflare.Backends.BufferProducer
  alias Logflare.Sources
  alias Logflare.Utils

  @producer_concurrency 1
  @processor_concurrency 5
  @batcher_concurrency 10
  @batch_size 1_500

  @doc false
  def child_spec(arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]}
    }
  end

  @doc false
  @spec start_link(list()) ::
          {:ok, pid()} | :ignore | {:error, {:already_started, pid()} | term()}
  def start_link(args) do
    {name, args} = Keyword.pop(args, :name)
    source = Keyword.get(args, :source)
    backend = Keyword.get(args, :backend)

    Broadway.start_link(__MODULE__,
      name: name,
      hibernate_after: 5_000,
      spawn_opt: [
        fullsweep_after: 10
      ],
      producer: [
        module: {BufferProducer, [source_id: source.id, backend_id: backend.id]},
        transformer: {__MODULE__, :transform, []},
        concurrency: @producer_concurrency
      ],
      processors: [
        default: [concurrency: @processor_concurrency, min_demand: 1, max_demand: 100]
      ],
      batchers: [
        ch: [concurrency: @batcher_concurrency, batch_size: @batch_size, batch_timeout: 1_500]
      ],
      context: %{
        source_id: source.id,
        source_token: source.token,
        backend_id: backend.id
      }
    )
  end

  # see the implementation for `Backends.via_source/2` for how tuples are used to identify child processes
  def process_name({:via, module, {registry, identifier}}, base_name) do
    new_identifier = Utils.append_to_tuple(identifier, base_name)
    {:via, module, {registry, new_identifier}}
  end

  def handle_message(_processor_name, message, _adaptor_state) do
    Message.put_batcher(message, :ch)
  end

  def handle_batch(:ch, messages, batch_info, %{
        source_id: source_id,
        source_token: source_token,
        backend_id: backend_id
      }) do
    :telemetry.execute(
      [:logflare, :backends, :pipeline, :handle_batch],
      %{batch_size: batch_info.size, batch_trigger: batch_info.trigger},
      %{
        backend_type: :clickhouse
      }
    )

    attributes =
      for {k, v} <- [
            source_id: source_id,
            source_token: source_token,
            backend_id: backend_id,
            ingest_batch_size: batch_info.size,
            ingest_batch_trigger: batch_info.trigger
          ],
          v != nil,
          do: {k, v}

    OpenTelemetry.Tracer.with_span :clickhouse_pipeline, %{
      attributes: Map.new(attributes)
    } do
      source = Sources.Cache.get_by_id(source_id)
      backend = Backends.Cache.get_backend(backend_id)
      events = for %{data: le} <- messages, do: le

      ClickhouseAdaptor.insert_log_events({source, backend}, events)
    end

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
