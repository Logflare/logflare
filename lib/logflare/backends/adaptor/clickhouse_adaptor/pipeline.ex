defmodule Logflare.Backends.Adaptor.ClickhouseAdaptor.Pipeline do
  @moduledoc """
  Pipeline for `ClickhouseAdaptor`

  This pipeline is responsible for taking log events from the
  source backend and inserting them into the configured database.
  """

  require Logger
  require OpenTelemetry.Tracer

  alias Broadway.Message
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.ClickhouseAdaptor
  alias Logflare.Backends.BufferProducer
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.LogEvent
  alias Logflare.Sources
  alias Logflare.Utils

  @producer_concurrency 1
  @processor_concurrency 5
  @batcher_concurrency 10
  @batch_size 1_500
  @max_retries 3

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
        transformer: {__MODULE__, :transform, [source_id: source.id, backend_id: backend.id]},
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

    result =
      OpenTelemetry.Tracer.with_span :clickhouse_pipeline, %{
        attributes: %{
          source_id: source_id,
          source_token: source_token,
          backend_id: backend_id,
          ingest_batch_size: batch_info.size,
          ingest_batch_trigger: batch_info.trigger
        }
      } do
        source = Sources.Cache.get_by_id(source_id)
        backend = Backends.Cache.get_backend(backend_id)
        events = for %{data: le} <- messages, do: le

        ClickhouseAdaptor.insert_log_events({source, backend}, events)
      end

    case result do
      :ok ->
        messages

      {:error, reason} ->
        Enum.map(messages, &Message.failed(&1, reason))
    end
  end

  def transform(event, opts) do
    %Message{
      data: event,
      acknowledger:
        {__MODULE__, :ack_id, %{source_id: opts[:source_id], backend_id: opts[:backend_id]}}
    }
  end

  def ack(_ack_ref, _successful, []), do: :ok

  def ack(_ack_ref, _successful, failed) do
    failed
    |> Enum.group_by(fn %{acknowledger: {_, _, ack_data}} -> ack_data end)
    |> Enum.each(fn
      {%{source_id: source_id, backend_id: backend_id}, messages}
      when is_integer(source_id) and is_integer(backend_id) ->
        {retriable, exhausted} =
          Enum.split_with(messages, fn %{data: event} ->
            (event.retries || 0) < @max_retries
          end)

        drop_exhausted_messages(exhausted, source_id, backend_id)
        requeue_retriable_messages(retriable, source_id, backend_id)

      {ack_data, messages} ->
        Logger.warning(
          "Dropping #{length(messages)} ClickHouse events with unexpected acknowledger data",
          error_string: inspect(ack_data)
        )
    end)
  end

  @spec drop_exhausted_messages(
          [Message.t()],
          source_id :: pos_integer(),
          backend_id :: pos_integer()
        ) :: :ok
  defp drop_exhausted_messages([], _source_id, _backend_id), do: :ok

  defp drop_exhausted_messages(exhausted, source_id, backend_id) do
    source = Sources.Cache.get_by_id(source_id)

    Logger.warning(
      "Dropping #{length(exhausted)} ClickHouse events after #{@max_retries} retries",
      source_token: source.token,
      backend_id: backend_id
    )

    events = Enum.map(exhausted, fn %{data: %LogEvent{} = event} -> event end)
    IngestEventQueue.delete_batch({source_id, backend_id}, events)
  end

  @spec requeue_retriable_messages(
          [Message.t()],
          source_id :: pos_integer(),
          backend_id :: pos_integer()
        ) ::
          :ok
  defp requeue_retriable_messages([], _source_id, _backend_id), do: :ok

  defp requeue_retriable_messages(retriable, source_id, backend_id) do
    source = Sources.Cache.get_by_id(source_id)

    events =
      Enum.map(retriable, fn %{data: %LogEvent{} = event} ->
        %LogEvent{event | retries: (event.retries || 0) + 1}
      end)

    Logger.info(
      "Requeuing #{length(events)} ClickHouse events for retry",
      source_token: source.token,
      backend_id: backend_id
    )

    IngestEventQueue.delete_batch({source_id, backend_id}, events)
    IngestEventQueue.add_to_table({source_id, backend_id}, events)
  end
end
