defmodule Logflare.Backends.SourceSup do
  use Supervisor

  alias Logflare.Backends.{SourceBackend}
  alias Logflare.Backends
  alias Logflare.Source
  alias Logflare.Buffers.MemoryBuffer

  def start_link(%Source{} = source) do
    Supervisor.start_link(__MODULE__, source, name: Backends.via_source(source, __MODULE__))
  end

  def init(source) do
    source_backend_specs =
      source
      |> Backends.list_source_backends()
      |> Enum.map(&SourceBackend.child_spec/1)

    children =
      [
        # {Stack, [:hello]}
        {MemoryBuffer, name: Backends.via_source(source, :buffer)},
        {__MODULE__.Pipeline, source}
      ] ++ source_backend_specs

    Supervisor.init(children, strategy: :one_for_one)
  end

  # Broadway Pipeline
  defmodule Pipeline do
    @moduledoc false
    use Broadway
    alias Broadway.Message
    alias Logflare.Buffers.BufferProducer
    alias Logflare.Source
    alias Logflare.Backends

    @spec start_link(Source.t()) :: {:ok, pid()}
    def start_link(%Source{} = source) do
      Broadway.start_link(__MODULE__,
        name: __MODULE__,
        producer: [
          module:
            {BufferProducer,
             buffer_module: MemoryBuffer, buffer_pid: Backends.via_source(source, :buffer)},
          transformer: {__MODULE__, :transform, []},
          concurrency: 1
        ],
        processors: [
          default: [concurrency: 1]
        ],
        batchers: [
          backends: [concurrency: 1, batch_size: 10],
        ],
        context: source
      )
    end

    def handle_message(_processor_name, message, source) do
      message
      |> Message.put_batcher(:backends)
    end

    def handle_batch(:backends, messages, _batch_info, source) do
      # dispatch messages to backends
      log_events = for %{data: msg} <- messages, do: msg
      Backends.dispatch_ingest(log_events, source)
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
end
