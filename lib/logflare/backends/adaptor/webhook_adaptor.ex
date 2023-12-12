defmodule Logflare.Backends.Adaptor.WebhookAdaptor do
  @moduledoc false

  use GenServer
  use TypedStruct

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.WebhookAdaptor
  alias Logflare.Backends.SourceBackend
  alias Logflare.Backends.SourceDispatcher
  alias Logflare.Buffers.Buffer
  alias Logflare.Buffers.MemoryBuffer

  @behaviour Logflare.Backends.Adaptor

  typedstruct enforce: true do
    field(:buffer_module, Adaptor.t())
    field(:buffer_pid, pid())
    field(:config, %{url: String.t()})
    field(:source_backend, SourceBackend.t())
    field(:pipeline_name, tuple())
  end

  # API

  @impl Logflare.Backends.Adaptor
  def start_link(%SourceBackend{} = source_backend) do
    GenServer.start_link(__MODULE__, source_backend,
      name: Backends.via_source_backend(source_backend, __MODULE__)
    )
  end

  @impl Logflare.Backends.Adaptor
  def ingest(pid, log_events), do: GenServer.call(pid, {:ingest, log_events})

  @impl Logflare.Backends.Adaptor
  def cast_config(params) do
    {%{}, %{url: :string}}
    |> Ecto.Changeset.cast(params, [:url])
  end

  @impl Logflare.Backends.Adaptor
  def execute_query(_ident, _query),
    do: {:error, :not_implemented}

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset) do
    changeset
    |> Ecto.Changeset.validate_required([:url])
    |> Ecto.Changeset.validate_format(:url, ~r/https?\:\/\/.+/)
  end

  # GenServer
  @impl GenServer
  def init(source_backend) do
    {:ok, _} =
      Registry.register(SourceDispatcher, source_backend.source_id, {__MODULE__, :ingest})

    {:ok, buffer_pid} = MemoryBuffer.start_link([])

    state = %__MODULE__{
      buffer_module: MemoryBuffer,
      buffer_pid: buffer_pid,
      config: source_backend.config,
      source_backend: source_backend,
      pipeline_name: Backends.via_source_backend(source_backend, __MODULE__.Pipeline)
    }

    {:ok, _pipeline_pid} = __MODULE__.Pipeline.start_link(state)
    {:ok, state}
  end

  @impl GenServer
  def handle_call({:ingest, log_events}, _from, %{config: _config} = state) do
    # TODO: queue, send concurrently
    Buffer.add_many(state.buffer_module, state.buffer_pid, log_events)
    {:reply, :ok, state}
  end

  # HTTP Client
  defmodule Client do
    @moduledoc false
    use Tesla, docs: false

    plug(Tesla.Middleware.Telemetry)
    plug(Tesla.Middleware.JSON)

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
    alias Logflare.Backends.Adaptor.WebhookAdaptor
    alias Logflare.Backends.Adaptor.WebhookAdaptor.Client

    @spec start_link(WebhookAdaptor.t()) :: {:ok, pid()}
    def start_link(adaptor_state) do
      Broadway.start_link(__MODULE__,
        name: adaptor_state.pipeline_name,
        producer: [
          module:
            {BufferProducer,
             buffer_module: adaptor_state.buffer_module, buffer_pid: adaptor_state.buffer_pid},
          transformer: {__MODULE__, :transform, []},
          concurrency: 1
        ],
        processors: [
          default: [concurrency: 3, min_demand: 1]
        ],
        context: adaptor_state
      )
    end

    # see the implementation for Backends.via_source_backend/2 for how tuples are used to identify child processes
    def process_name({:via, module, {registry, identifier}}, base_name) do
      new_identifier = Tuple.append(identifier, base_name)
      {:via, module, {registry, new_identifier}}
    end

    def handle_message(_processor_name, message, adaptor_state) do
      Message.update_data(message, &process_data(&1, adaptor_state))
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
