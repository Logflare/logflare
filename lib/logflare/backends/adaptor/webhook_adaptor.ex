defmodule Logflare.Backends.Adaptor.WebhookAdaptor do
  @moduledoc false

  use GenServer
  use TypedStruct

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.WebhookAdaptor
  alias Logflare.Backends.Backend

  @behaviour Logflare.Backends.Adaptor

  typedstruct do
    field(:config, %{
      url: String.t(),
      headers: map(),
      http: String.t()
    })

    field(:backend, Backend.t())
    field(:pipeline_name, tuple())
    field(:backend_token, String.t())
    field(:source_token, atom())
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

    Backends.via_source(source_id, __MODULE__.Pipeline, backend_id)
    |> Broadway.push_messages(messages)
  end

  @impl Logflare.Backends.Adaptor
  def cast_config(params) do
    {%{}, %{url: :string, headers: :map, http: :string}}
    |> Ecto.Changeset.cast(params, [:url, :headers, :http])
  end

  @impl Logflare.Backends.Adaptor
  def execute_query(_ident, _query),
    do: {:error, :not_implemented}

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset) do
    changeset
    |> Ecto.Changeset.validate_required([:url, :http])
    |> Ecto.Changeset.validate_format(:url, ~r/https?\:\/\/.+/)
  end

  # GenServer
  @impl GenServer
  def init({source, backend}) do
    :ok = Backends.register_backend_for_ingest_dispatch(source, backend)

    state = %__MODULE__{
      config: backend.config,
      backend: backend,
      backend_token: if(backend, do: backend.token, else: nil),
      source_token: source.token,
      pipeline_name: Backends.via_source(source, __MODULE__.Pipeline, backend.id)
    }

    {:ok, _pipeline_pid} = __MODULE__.Pipeline.start_link(state)
    {:ok, state}
  end

  # HTTP Client
  defmodule Client do
    @moduledoc false
    use Tesla, docs: false

    def send(opts) do
      adaptor =
        if Keyword.get(opts, :http) == "http1" do
          {Tesla.Adapter.Finch, name: Logflare.FinchDefaultHttp1, receive_timeout: 5_000}
        else
          {Tesla.Adapter.Finch, name: Logflare.FinchDefault, receive_timeout: 5_000}
        end

      opts =
        opts
        |> Keyword.put_new(:method, :post)
        |> Keyword.update(:headers, [], &Map.to_list/1)

      Tesla.client(
        [
          Tesla.Middleware.Telemetry,
          Tesla.Middleware.JSON
        ],
        adaptor
      )
      |> request(opts)
    end
  end

  # Broadway Pipeline
  defmodule Pipeline do
    @moduledoc false
    use Broadway
    alias Broadway.Message
    alias Logflare.Backends.BufferProducer
    alias Logflare.Backends.Adaptor.WebhookAdaptor
    alias Logflare.Backends.Adaptor.WebhookAdaptor.Client

    @spec start_link(WebhookAdaptor.t()) :: {:ok, pid()}
    def start_link(adaptor_state) do
      Broadway.start_link(__MODULE__,
        name: adaptor_state.pipeline_name,
        hibernate_after: 5_000,
        spawn_opt: [
          fullsweep_after: 100
        ],
        producer: [
          module:
            {BufferProducer,
             [
               source_token: adaptor_state.source_token,
               backend_token: adaptor_state.backend_token
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
        context: adaptor_state
      )
    end

    # see the implementation for Backends.via_source/2 for how tuples are used to identify child processes
    def process_name({:via, module, {registry, identifier}}, base_name) do
      new_identifier = Tuple.append(identifier, base_name)
      {:via, module, {registry, new_identifier}}
    end

    def handle_message(_processor_name, message, _adaptor_state) do
      message
      |> Message.put_batcher(:http)
    end

    def handle_batch(:http, messages, _batch_info, context) do
      payload = for %{data: le} <- messages, do: le.body
      process_data(payload, context)
      messages
    end

    defp process_data(log_event_bodies, %{config: %{} = config}) do
      Client.send(
        url: config.url,
        body: log_event_bodies,
        headers: config[:headers] || %{},
        http: config.http
      )
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
