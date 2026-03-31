defmodule Logflare.Backends.Adaptor.SyslogAdaptor.Pipeline do
  @moduledoc false
  use Broadway

  @behaviour Broadway.Acknowledger

  alias Broadway.Message
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.SyslogAdaptor.{Pool, Syslog}

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

  defp fail_batch(messages, reason) do
    Enum.map(messages, fn message ->
      Broadway.Message.failed(message, reason)
    end)
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
