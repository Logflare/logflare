defmodule Logflare.Backends.Adaptor.GoogleAnalyticsAdaptor do
  @moduledoc false
  use GenServer
  alias Logflare.Backends.SourceBackend
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.GoogleAnalyticsAdaptor
  alias Logflare.Backends.SourceDispatcher
  alias Logflare.Backends
  alias Logflare.Buffers.MemoryBuffer
  use Adaptor
  use TypedStruct

  typedstruct enforce: true do
    field :buffer_module, Adaptor.t()
    field :buffer_pid, pid()

    field :config, %{
      event_name_paths: String.t(),
      client_id_path: String.t(),
      api_secret: String.t(),
      measurement_id: String.t()
    }

    field :source_backend, SourceBackend.t()
    field :pipeline_name, tuple()
  end

  def start_link(%SourceBackend{} = source_backend) do
    GenServer.start_link(__MODULE__, source_backend)
  end

  @impl true
  def init(source_backend) do
    {:ok, _} =
      Registry.register(
        SourceDispatcher,
        source_backend.source_id,
        {GoogleAnalyticsAdaptor, :ingest}
      )

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

  # API

  @impl Adaptor
  def ingest(pid, log_events), do: GenServer.call(pid, {:ingest, log_events})

  @impl Adaptor
  def cast_config(params) do
    {%{},
     %{
       event_name_paths: :string,
       client_id_path: :string,
       api_secret: :string,
       measurement_id: :string
     }}
    |> Ecto.Changeset.cast(params, [
      :event_name_paths,
      :client_id_path,
      :api_secret,
      :measurement_id
    ])
  end

  @impl Adaptor
  def validate_config(changeset) do
    changeset
    |> Ecto.Changeset.validate_required([
      :event_name_paths,
      :client_id_path,
      :api_secret,
      :measurement_id
    ])
  end

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
    alias Logflare.Backends.Adaptor.{GoogleAnalyticsAdaptor, GoogleAnalyticsAdaptor.Client}

    @spec start_link(GoogleAnalyticsAdaptor.t()) :: {:ok, pid()}
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
      message
      |> Message.update_data(&process_data(&1, adaptor_state))
    end

    defp process_data(log_event, %{config: config}) do
      query_map = config |> Map.take([:measurement_id, :api_secret])
      query_string = URI.encode_query(query_map)

      uri =
        URI.parse("https://www.google-analytics.com/mp/collect?")
        |> Map.put(:query, query_string)

      url = URI.to_string(uri)

      client_id =
        case Warpath.query(log_event.body, "$.#{config.client_id_path}") do
          {:ok, v} ->
            v

          {:error, _} ->
            raise "json_path_query_error"
        end

      names =
        config.event_name_paths
        |> String.split(",", trim: true)
        |> Enum.map(&String.trim/1)
        |> Enum.map(fn path ->
          case Warpath.query(log_event.body, "$.#{path}") do
            {:ok, v} ->
              v

            {:error, _} ->
              raise "json_path_query_error"
          end
        end)

      events =
        for name <- names do
          %{
            "name" => name,
            "params" => log_event.body
          }
        end

      body = %{
        "client_id" => client_id,
        "events" => events
      }

      Client.send(url, body)
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
