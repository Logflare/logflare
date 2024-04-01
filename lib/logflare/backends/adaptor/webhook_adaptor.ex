defmodule Logflare.Backends.Adaptor.WebhookAdaptor do
  @moduledoc false

  use GenServer
  use TypedStruct

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.WebhookAdaptor
  alias Logflare.Backends.Backend

  @behaviour Logflare.Backends.Adaptor

  typedstruct enforce: true do
    field(:config, %{
      url: String.t(),
      headers: map()
    })

    field(:backend, Backend.t())
    field(:pipeline_name, tuple())
  end

  # API

  @impl Logflare.Backends.Adaptor
  def start_link({source, backend} = args) do
    GenServer.start_link(__MODULE__, args,
      name: Backends.via_source(source, __MODULE__, backend.id)
    )
  end

  @impl Logflare.Backends.Adaptor
  def ingest(_pid, log_events, opts \\ []) do
    source_id = Keyword.get(opts, :source_id)
    backend_id = Keyword.get(opts, :backend_id)
    messages = Enum.map(log_events, &__MODULE__.Pipeline.transform(&1, []))

    Backends.via_source(source_id, {__MODULE__.Pipeline, backend_id})
    |> Broadway.push_messages(messages)
  end

  @impl Logflare.Backends.Adaptor
  def cast_config(params) do
    {%{}, %{url: :string, headers: :map}}
    |> Ecto.Changeset.cast(params, [:url, :headers])
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
  def init({source, backend}) do
    :ok = Backends.register_backend_for_ingest_dispatch(source, backend)

    state = %__MODULE__{
      config: backend.config,
      backend: backend,
      pipeline_name: Backends.via_source(source, __MODULE__.Pipeline, backend.id)
    }

    {:ok, _pipeline_pid} = __MODULE__.Pipeline.start_link(state)
    {:ok, state}
  end

  # HTTP Client
  defmodule Client do
    @moduledoc false
    use Tesla, docs: false

    plug(Tesla.Middleware.Telemetry)
    plug(Tesla.Middleware.JSON)

    def send(opts) do
      opts
      |> Keyword.put_new(:method, :post)
      |> Keyword.update(:headers, [], &Map.to_list/1)
      |> request()
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
          module: {BufferProducer, []},
          transformer: {__MODULE__, :transform, []},
          concurrency: 1
        ],
        processors: [
          default: [concurrency: 3, min_demand: 1]
        ],
        context: adaptor_state
      )
    end

    # see the implementation for Backends.via_source/2 for how tuples are used to identify child processes
    def process_name({:via, module, {registry, identifier}}, base_name) do
      new_identifier = Tuple.append(identifier, base_name)
      {:via, module, {registry, new_identifier}}
    end

    def handle_message(_processor_name, message, adaptor_state) do
      Message.update_data(message, &process_data(&1, adaptor_state))
    end

    defp process_data(log_event, %{config: %{} = config}) do
      Client.send(url: config.url, body: log_event.body, headers: config[:headers] || %{})
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
