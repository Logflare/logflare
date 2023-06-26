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
  alias Logflare.Backends.Adaptor.PostgresAdaptor.Repo

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
         repository_module <- Repo.new_repository_for_source_backend(source_backend),
         :ok <- Repo.connect_to_source_backend(repository_module, source_backend),
         :ok <- Repo.create_log_event_table(repository_module) do
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

  # GenServer
  @impl true
  def handle_call({:ingest, log_events}, _from, %{config: _config} = state) do
    MemoryBuffer.add_many(state.buffer_pid, log_events)
    {:reply, :ok, state}
  end
end
