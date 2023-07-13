defmodule Logflare.Backends.Adaptor.PostgresAdaptor do
  @moduledoc """
  The PostgresAdaptor is a backend adaptor for the Postgres database.

  ## Configuration
  We store the PSQL URL address to whom we will be connected to
  ## How it works
  ### On Source Backend creation:
  * Broadway pipeline for ingestion: Logflare.Backends.Adaptor.PostgresAdaptor.Pipeline
  * MemoryBuffer for buffering log events: Logflare.Buffers.MemoryBuffer
  * Dynamically created Ecto.Repo created for configured PSQL URL: Logflare.Backends.Adaptor.PostgresAdaptor.Repo.create_repo
  * Dynamically loaded Ecto.Repo connects: Logflare.Backends.Adaptor.PostgresAdaptor.Repo.connect_to_repo
  * Dynamically loaded Ecto.Repo runs migrations required to work: Logflare.Backends.Adaptor.PostgresAdaptor.Repo.create_log_events_table

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
  alias __MODULE__.Pipeline
  alias __MODULE__.PgRepo

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
         repository_module <- create_repo(source_backend),
         :ok <- connect_to_repo(source_backend),
         :ok <- create_log_events_table(source_backend) do
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

  # expose PgRepo functions
  defdelegate create_repo(source_backend), to: PgRepo
  defdelegate connect_to_repo(source_backend), to: PgRepo
  defdelegate table_name(source_or_source_backend), to: PgRepo
  defdelegate create_log_events_table(source_backend), to: PgRepo
  defdelegate create_log_events_table(source_backend, override_migrations), to: PgRepo
  defdelegate rollback_migrations(source_backend), to: PgRepo
  defdelegate drop_migrations_table(source_backend), to: PgRepo
  defdelegate migrations_table_name(source_backend), to: PgRepo

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

  @impl true
  def handle_call({:execute_query, query_string}, _from, state) when is_binary(query_string) do
    mod = state.repository_module
    result = Ecto.Adapters.SQL.query!(mod, query_string)

    rows =
      for row <- result.rows do
        for {cell, index} <- Enum.with_index(row), into: %{} do
          {Enum.at(result.columns, index), cell}
        end
      end

    {:reply, rows, state}
  end
end
