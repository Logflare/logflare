defmodule Logflare.Backends.Adaptor.SyslogAdaptor.Pipeline do
  @moduledoc false
  use Broadway

  @behaviour Broadway.Acknowledger

  alias Broadway.Message
  alias Logflare.Backends
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Sources
  alias Logflare.Backends.Adaptor.SyslogAdaptor.{Pool, Syslog}

  require Logger

  @max_retries 1

  def start_link(opts) do
    backend = Keyword.fetch!(opts, :backend)
    source = Keyword.fetch!(opts, :source)
    pool = Keyword.fetch!(opts, :pool)
    name = Keyword.fetch!(opts, :name)

    Broadway.start_link(__MODULE__,
      name: name,
      producer: [
        module: {Backends.BufferProducer, backend_id: backend.id, source_id: source.id},
        transformer: {__MODULE__, :transform, []}
      ],
      processors: [
        default: [min_demand: 1]
      ],
      batchers: [
        syslog: [concurrency: 5, batch_size: 50]
      ],
      context: %{
        source_id: source.id,
        backend_id: backend.id,
        pool: pool
      }
    )
  end

  @impl Broadway
  def handle_message(_processor_name, message, _context) do
    Message.put_batcher(message, :syslog)
  end

  @impl Broadway
  def handle_batch(:syslog, messages, _batch_info, context) do
    %{pool: pool, backend_id: backend_id} = context
    config = lookup_backend_config(backend_id)

    content =
      for %Broadway.Message{data: log_event} <- messages do
        Syslog.format(log_event, config)
      end

    case Pool.send(pool, content) do
      :ok -> messages
      {:error, reason} -> fail_batch(messages, reason)
    end
  end

  @impl Broadway
  def handle_failed(messages, context) do
    %{source_id: source_id, backend_id: backend_id} = context

    {retriable, exhausted} =
      partition_retriable_events(messages, _retriable = [], _exhausted = [])

    source = Sources.Cache.get_by_id(source_id)
    if retriable != [], do: requeue_retriable_events(retriable, source, backend_id)
    if exhausted != [], do: drop_exhausted_events(exhausted, source, backend_id)

    messages
  end

  defp fail_batch(messages, reason) do
    Enum.map(messages, fn message ->
      Broadway.Message.failed(message, reason)
    end)
  end

  defp partition_retriable_events([message | messages], retriable, exhausted) do
    %Broadway.Message{data: %Logflare.LogEvent{retries: retries} = event} = message
    retries = retries || 0

    if retries < @max_retries do
      event = %{event | retries: retries + 1}
      partition_retriable_events(messages, [event | retriable], exhausted)
    else
      partition_retriable_events(messages, retriable, [event | exhausted])
    end
  end

  defp partition_retriable_events([], retriable, exhausted) do
    {:lists.reverse(retriable), :lists.reverse(exhausted)}
  end

  defp requeue_retriable_events(events, source, backend_id) do
    Logger.info(
      "Requeuing #{length(events)} Syslog events for retry",
      source_token: source.token,
      backend_id: backend_id
    )

    IngestEventQueue.delete_batch({source.id, backend_id}, events)
    IngestEventQueue.add_to_table({source.id, backend_id}, events)
  end

  defp drop_exhausted_events(events, source, backend_id) do
    Logger.warning(
      "Dropping #{length(events)} Syslog events after #{@max_retries} retries",
      source_token: source.token,
      backend_id: backend_id
    )

    IngestEventQueue.delete_batch({source.id, backend_id}, events)
  end

  @impl Broadway
  def process_name({:via, module, {registry, identifier}}, base_name) do
    new_identifier = Logflare.Utils.append_to_tuple(identifier, base_name)
    {:via, module, {registry, new_identifier}}
  end

  @impl Broadway.Acknowledger
  def ack(_ack_ref, _successful, _failed), do: :ok

  @doc false
  def transform(event, _opts) do
    %Message{data: event, acknowledger: {__MODULE__, _ref = nil, _meta = []}}
  end

  defp lookup_backend_config(backend_id) do
    %{config: config} =
      Logflare.Backends.Cache.get_backend(backend_id) || raise "missing backend #{backend_id}"

    if cipher_key = config[:cipher_key] do
      Map.put(config, :cipher_key, Base.decode64!(cipher_key))
    else
      config
    end
  end
end
