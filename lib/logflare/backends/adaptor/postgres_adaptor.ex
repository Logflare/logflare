defmodule Logflare.Backends.Adaptor.PostgresAdaptor do
  @moduledoc false
  use GenServer
  alias Logflare.Backends.{SourceBackend, Adaptor, Adaptor.PostgresAdaptor, SourceDispatcher}
  alias Logflare.{Backends, Buffers.MemoryBuffer}
  use Adaptor
  use TypedStruct

  typedstruct enforce: true do
    field :buffer_module, Adaptor.t()
    field :buffer_pid, pid()

    field :config, %{
      url: String.t()
    }

    field :source_backend, SourceBackend.t()
    field :pipeline_name, tuple()
    field :repo_pid, pid()

  end

  def start_link(%SourceBackend{} = source_backend) do
    GenServer.start_link(__MODULE__, source_backend)
  end

  @impl true
  def init(source_backend) do
    {:ok, _} =
      Registry.register(SourceDispatcher, source_backend.source_id, {PostgresAdaptor, :ingest})

    {:ok, buffer_pid} = MemoryBuffer.start_link([])

    {:ok, repo} = __MODULE__.Repo.start_link([
      name: Backends.via_source_backend(source_backend, __MODULE__.Repo),
      hostname: source_backend.config.db_host,
      port: source_backend.config.db_port,
      username: source_backend.config.db_username,
      password: source_backend.config.db_password,
      database: source_backend.config.db_database,
    ])
    IO.inspect("repo")
    IO.inspect(repo)

    state = %__MODULE__{
      buffer_module: MemoryBuffer,
      buffer_pid: buffer_pid,
      config: source_backend.config,
      source_backend: source_backend,
      pipeline_name: Backends.via_source_backend(source_backend, __MODULE__.Pipeline),
      repo_pid: repo
    }

    {:ok, _pipeline_pid} = __MODULE__.Pipeline.start_link(state)
    {:ok, state}
  end

  # API
  @impl Adaptor
  def ingest(pid, log_events), do: GenServer.call(pid, {:ingest, log_events})

  @impl Adaptor
  def cast_config(params) do
    {%{}, %{
      db_host: :string,
      db_password: :string,
      db_username: :string,
      db_database: :string,
      db_port: :integer,
    }}
    |> Ecto.Changeset.cast(params, [:db_host, :db_password, :db_username, :db_database, :db_port])
    |> Ecto.Changeset.validate_required([:db_host, :db_password, :db_username, :db_database, :db_port])
  end

  @impl Adaptor
  def validate_config(changeset) do
    changeset
    |> Ecto.Changeset.validate_required([:url])
    |> Ecto.Changeset.validate_format(:url, ~r/https?\:\/\/.+/)
  end

  # GenServer
  @impl true
  def handle_call({:ingest, log_events}, _from, %{config: _config} = state) do
    # TODO: queue, send concurrently
    MemoryBuffer.add_many(state.buffer_pid, log_events)
    {:reply, :ok, state}
  end

  # Ecto Repo
  defmodule Repo do
    @moduledoc false
    use Ecto.Repo,
      otp_app: :logflare,
      adapter: Ecto.Adapters.Postgres
    def ingest(data) do
      IO.inspect("performing ingestion #{inspect(data)}")
    end
  end



  # Broadway Pipeline
  defmodule Pipeline do
    @moduledoc false
    use Broadway
    alias Broadway.Message
    alias Logflare.Buffers.BufferProducer
    alias Logflare.Backends.Adaptor.{PostgresAdaptor, PostgresAdaptor.Repo}

    @spec start_link(PostgresAdaptor.t()) :: {:ok, pid()}
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

    defp process_data(log_event, %{repo_pid: repo_pid}) do
      # perform the ingest
      Repo.put_dynamic_repo(repo_pid)
      Repo.ingest(log_event.body)
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
