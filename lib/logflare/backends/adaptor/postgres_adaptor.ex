defmodule Logflare.Backends.Adaptor.PostgresAdaptor do
  @moduledoc """
  The backend adaptor for the Postgres database.

  Config:
  `:url` - the database connection string

  On ingest, pipeline will insert it into the log event table for the given source.
  """
  use GenServer
  use TypedStruct

  alias Logflare.Backends.Adaptor.PostgresAdaptor.Pipeline
  alias Logflare.Backends.Adaptor.PostgresAdaptor.PgRepo
  alias Logflare.Backends
  alias Logflare.Backends.SourceBackend
  alias Logflare.Backends.SourceDispatcher
  alias Logflare.Buffers.Buffer
  alias Logflare.Buffers.MemoryBuffer

  @behaviour Logflare.Backends.Adaptor

  import Ecto.Changeset

  typedstruct enforce: true do
    field(:buffer_module, Adaptor.t())
    field(:buffer_pid, pid())
    field(:config, %{url: String.t(), schema: String.t()})
    field(:source_backend, SourceBackend.t())
    field(:pipeline_name, tuple())
    field(:repository_module, tuple())
  end

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
    {%{}, %{url: :string, schema: :string}}
    |> cast(params, [:url, :schema])
  end

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset) do
    changeset
    |> validate_required([:url])
    |> validate_format(:url, ~r/postgresql?\:\/\/.+/)
  end

  @doc """
  Executes either an Ecto.Query or an sql string on the Postgres backend.

  If an sql string is provided, one can also provide parameters to be passed.
  Parameter placeholders should correspond to Postgres format, i.e. `$#`

  ### Examples
    iex> execute_query(souce_backend, from(s in "log_event_..."))
    {:ok, [%{...}]}
    iex> execute_query(source_backend, "select body from log_event_table")
    {:ok, [%{...}]}
    iex> execute_query(source_backend, {"select $1 as c from log_event_table", ["value]})
    {:ok, [%{...}]}
  """
  @impl Logflare.Backends.Adaptor
  def execute_query(%SourceBackend{} = source_backend, %Ecto.Query{} = query) do
    mod = create_repo(source_backend)
    :ok = connected?(source_backend)

    result =
      query
      |> mod.all()
      |> Enum.map(&nested_map_update/1)

    {:ok, result}
  end

  def execute_query(%SourceBackend{} = source_backend, query_string) when is_binary(query_string),
    do: execute_query(source_backend, {query_string, []})

  def execute_query(%SourceBackend{config: config} = source_backend, {query_string, params})
      when is_binary(query_string) and is_list(params) do
    mod = create_repo(source_backend)
    :ok = connected?(source_backend)

    # explicitly set search path
    schema = Map.get(config, "schema") || Map.get(config, :schema)

    if schema do
      Ecto.Adapters.SQL.query!(mod, "SET search_path=#{schema}")
    end

    result = Ecto.Adapters.SQL.query!(mod, query_string, params)

    rows =
      for row <- result.rows do
        result.columns
        |> Enum.zip(row)
        |> Map.new()
        |> nested_map_update()
      end

    {:ok, rows}
  end

  defp nested_map_update(value) when is_struct(value), do: value

  defp nested_map_update(value) when is_map(value),
    do: Enum.reduce(value, %{}, &nested_map_update/2)

  defp nested_map_update(value) when is_list(value), do: Enum.map(value, &nested_map_update/1)

  defp nested_map_update(value), do: value

  defp nested_map_update({key, value}, acc) when is_map(value) do
    Map.put(acc, key, [nested_map_update(value)])
  end

  defp nested_map_update({key, value}, acc) do
    Map.put(acc, key, nested_map_update(value))
  end

  # expose PgRepo functions
  defdelegate connected?(source_backend), to: PgRepo
  defdelegate create_repo(source_backend), to: PgRepo
  defdelegate table_name(source_or_source_backend), to: PgRepo
  defdelegate create_log_events_table(source_backend), to: PgRepo
  defdelegate create_log_events_table(source_backend, override_migrations), to: PgRepo
  defdelegate rollback_migrations(source_backend), to: PgRepo
  defdelegate drop_migrations_table(source_backend), to: PgRepo
  defdelegate migrations_table_name(source_backend), to: PgRepo
  defdelegate insert_log_event(source_backend, log_event), to: PgRepo

  # GenServer
  @impl GenServer
  def init(source_backend) do
    with source_id = source_backend.source_id,
         {:ok, _} <- Registry.register(SourceDispatcher, source_id, {__MODULE__, :ingest}),
         {:ok, buffer_pid} <- MemoryBuffer.start_link([]),
         repository_module = create_repo(source_backend),
         :ok <- connected?(source_backend),
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

  @impl GenServer
  def handle_call({:ingest, log_events}, _from, %{config: _config} = state) do
    Buffer.add_many(state.buffer_module, state.buffer_pid, log_events)
    {:reply, :ok, state}
  end
end
