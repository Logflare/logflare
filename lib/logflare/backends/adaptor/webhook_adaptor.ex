defmodule Logflare.Backends.Adaptor.WebhookAdaptor do
  @moduledoc false
  use GenServer
  alias Logflare.Backends.{SourceBackend, Adaptor, Adaptor.WebhookAdaptor, SourceDispatcher}
  alias Logflare.Buffers.MemoryBuffer
  use Adaptor
  use TypedStruct

  typedstruct do
    field :buffer_module, Adaptor.t(), enforce: true
    field :buffer_pid, pid(), enforce: true
    field :config, map, enforce: true
  end

  def start_link(%SourceBackend{} = source_backend) do
    GenServer.start_link(__MODULE__, source_backend)
  end

  @impl true
  def init(source_backend) do
    {:ok, _} =
      Registry.register(SourceDispatcher, source_backend.source_id, {WebhookAdaptor, :ingest})

    {:ok, buffer_pid} = MemoryBuffer.start_link([])

    state = %__MODULE__{
      buffer_module: MemoryBuffer,
      buffer_pid: buffer_pid,
      config: source_backend.config
    }

    {:ok, _pipeline_pid} = __MODULE__.Pipeline.start_link(state)
    {:ok, state}
  end

  # API

  @impl Adaptor
  def ingest(pid, log_events), do: GenServer.call(pid, {:ingest, log_events})

  # GenServer
  @impl true
  def handle_call({:ingest, log_events}, _from, %{config: _config} = state) do
    # TODO: queue, send concurrently
    MemoryBuffer.add_many(state.buffer_pid, log_events)
    {:reply, :ok, state}
  end

  # HTTP Client
  defmodule Client do
    @moduledoc false
    use Tesla, docs: false

    # plug Tesla.Middleware.Headers, [{"authorization", "token xyz"}]
    plug Tesla.Middleware.JSON

    def send(url, body, opts \\ %{}) do
      opts = Enum.into(opts, %{method: :post})
      request(method: opts.method, url: url, body: body)
    end
  end

  # Broadway Pipeline
  defmodule Pipeline do
    @moduledoc false
    use Broadway
    alias Broadway.Message
    alias Logflare.Buffers.BufferProducer
    alias Logflare.Backends.Adaptor.WebhookAdaptor.Client
    alias Logflare.Backends.Adaptor.WebhookAdaptor

    @spec start_link(WebhookAdaptor.t()) :: {:ok, pid()}
    def start_link(adaptor_state) do
      Broadway.start_link(__MODULE__,
        name: __MODULE__,
        producer: [
          module:
            {BufferProducer,
             buffer_module: adaptor_state.buffer_module, buffer_pid: adaptor_state.buffer_pid},
          transformer: {__MODULE__, :transform, []},
          concurrency: 1
        ],
        processors: [
          default: [concurrency: 5, min_demand: 1]
        ],
        context: adaptor_state
      )
    end

    def handle_message(_processor_name, message, adaptor_state) do
      message
      |> Message.update_data(&process_data(&1, adaptor_state))
    end

    defp process_data(log_event, %{config: %{url: url}}) do
      Client.send(url, log_event.body)
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
