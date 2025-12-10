defmodule Logflare.Backends.Adaptor.TCPAdaptor.Pipeline do
  @moduledoc false
  use Broadway

  @behaviour Broadway.Acknowledger

  alias Broadway.Message
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.TCPAdaptor.{Pool, Syslog}

  def start_link(opts) do
    backend = Keyword.fetch!(opts, :backend)
    source = Keyword.fetch!(opts, :source)
    pool = Keyword.fetch!(opts, :pool)
    name = Keyword.fetch!(opts, :name)

    cipher_key =
      if cipher_key = backend.config[:cipher_key] do
        Base.decode64!(cipher_key)
      end

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
        tcp: [concurrency: 5, batch_size: 50]
      ],
      context: %{
        source_id: source.id,
        backend_id: backend.id,
        pool: pool,
        cipher_key: cipher_key
      }
    )
  end

  @impl Broadway
  def handle_message(_processor_name, message, _context) do
    Message.put_batcher(message, :tcp)
  end

  @impl Broadway
  def handle_batch(:tcp, messages, _batch_info, context) do
    %{pool: pool, cipher_key: cipher_key} = context

    content =
      for %Broadway.Message{data: log_event} <- messages do
        Syslog.format(log_event, cipher_key)
      end

    Pool.send(pool, content)
    messages
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
end
