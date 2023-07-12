defmodule Logflare.Backends.Adaptor.PostgresAdaptor do
  @moduledoc """
  The PostgresAdaptor is a backend adaptor for the Postgres database.

  ## Configuration
  We store the PSQL URL address to whom we will be connected to
  ## How it works
  ### On Source Backend creation:
  * Broadway pipeline for ingestion: Logflare.Backends.Adaptor.PostgresAdaptor.Pipeline
  * MemoryBuffer for buffering log events: Logflare.Buffers.MemoryBuffer
  * Dynamically created Ecto.Repo created for configured PSQL URL: Logflare.Backends.Adaptor.PostgresAdaptor.Repo.new_repository_for_source_backend
  * Dynamically loaded Ecto.Repo connects: Logflare.Backends.Adaptor.PostgresAdaptor.Repo.connect_to_source_backend
  * Dynamically loaded Ecto.Repo runs migrations required to work: Logflare.Backends.Adaptor.PostgresAdaptor.Repo.create_log_event_table

  ## On LogEvent ingestion:
  On a new event, the Postgres Pipeline will consume the event and store it into the dynamically loaded Logflare.Backends.Adaptor.PostgresAdaptor.Repo.
  """
  use GenServer
  use TypedStruct
  use Logflare.Backends.Adaptor

  alias Logflare.Backends
  alias Logflare.Backends.SourceBackend
  alias Logflare.Backends.SourceDispatcher
  alias Logflare.Buffers.MemoryBuffer
  alias Logflare.Backends.Adaptor.PostgresAdaptor.Pipeline

  typedstruct enforce: true do
    field(:buffer_module, Adaptor.t())
    field(:buffer_pid, pid())
    field(:config, %{url: String.t()})
    field(:source_backend, SourceBackend.t())
    field(:pipeline_name, tuple())
    field(:repository_module, tuple())
  end

  def start_link(%SourceBackend{} = source_backend) do
    GenServer.start_link(__MODULE__, source_backend)
  end

  @impl true
  def init(source_backend) do
    with source_id <- source_backend.source_id,
         {:ok, _} <- Registry.register(SourceDispatcher, source_id, {__MODULE__, :ingest}),
         {:ok, buffer_pid} <- MemoryBuffer.start_link([]),
         repository_module <- __MODULE__.Repo.new_repository_for_source_backend(source_backend),
         :ok <- __MODULE__.Repo.connect_to_source_backend(repository_module, source_backend),
         :ok <- __MODULE__.Repo.create_log_event_table(repository_module, source_backend) do
      state = %__MODULE__{
        buffer_module: MemoryBuffer,
        buffer_pid: buffer_pid,
        config: source_backend.config,
        source_backend: source_backend,
        pipeline_name: Backends.via_source_backend(source_backend, Pipeline),
        repository_module: repository_module
      }

      {:ok, _pipeline_pid} = Pipeline.start_link(state)
      {:ok, state}
    end
  end

  @impl true
  def ingest(pid, log_events), do: GenServer.call(pid, {:ingest, log_events})

  @impl true
  def cast_config(params) do
    {%{}, %{url: :string}}
    |> Ecto.Changeset.cast(params, [:url])
  end

  @impl true
  def validate_config(changeset) do
    changeset
    |> Ecto.Changeset.validate_required([:url])
    |> Ecto.Changeset.validate_format(:url, ~r/postgresql?\:\/\/.+/)
  end

  @impl true
  def queryable?(), do: true

  @impl true
  def execute_query(pid, query) do
    GenServer.call(pid, {:execute_query, query})
  end

  @doc """
  Rolls back all migrations
  """
  @spec rollback_migrations(SourceBackend.t()) :: :ok
  def rollback_migrations(source_backend) do
    repository_module = __MODULE__.Repo.new_repository_for_source_backend(source_backend)

    Ecto.Migrator.run(
      repository_module,
      __MODULE__.Repo.migrations(source_backend),
      :down,
      all: true
    )

    :ok
    # GenServer.call(pid, :rollback_migrations)
  end

  @doc """
  Drops the migration table
  """
  @spec drop_migrations_table(SourceBackend.t()) :: :ok
  def drop_migrations_table(source_backend) do
    repository_module = __MODULE__.Repo.new_repository_for_source_backend(source_backend)
    migrations_table = migrations_table_name(source_backend)
    Ecto.Adapters.SQL.query!(repository_module, "DROP TABLE IF EXISTS #{migrations_table}")
    :ok
  end

  @doc """
  Returns the migrations table name used for a given source
  """
  @spec migrations_table_name(SourceBackend.t()) :: String.t()
  def migrations_table_name(%SourceBackend{source_id: source_id}) do
    "schema_migrations_#{source_id}"
  end

  # GenServer
  @impl true
  def handle_call({:ingest, log_events}, _from, %{config: _config} = state) do
    MemoryBuffer.add_many(state.buffer_pid, log_events)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:execute_query, %Ecto.Query{} = query}, _from, state) do
    mod = state.repository_module
    result = mod.all(query)
    {:reply, result, state}
  end
end
