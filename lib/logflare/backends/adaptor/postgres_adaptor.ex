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
  alias Logflare.Backends.Backend

  @behaviour Logflare.Backends.Adaptor

  import Ecto.Changeset

  typedstruct enforce: true do
    field(:config, %{
      url: String.t(),
      schema: String.t(),
      username: String.t(),
      password: String.t(),
      hostname: String.t(),
      port: non_neg_integer()
    })

    field(:source, Source.t())
    field(:backend, Backend.t())
    field(:pipeline_name, tuple())
  end

  @impl Logflare.Backends.Adaptor
  def start_link({source, backend}) do
    GenServer.start_link(__MODULE__, {source, backend},
      name: Backends.via_source(source, __MODULE__, backend.id)
    )
  end

  @impl Logflare.Backends.Adaptor
  def ingest(_pid, log_events, opts) do
    source_id = Keyword.get(opts, :source_id)
    backend_id = Keyword.get(opts, :backend_id)
    messages = Enum.map(log_events, &__MODULE__.Pipeline.transform(&1, []))
    pipeline_name = Backends.via_source(source_id, {Pipeline, backend_id})
    Broadway.push_messages(pipeline_name, messages)
  end

  @impl Logflare.Backends.Adaptor
  def cast_config(params) do
    {%{},
     %{
       url: :string,
       username: :string,
       password: :string,
       hostname: :string,
       database: :string,
       schema: :string,
       port: :integer
     }}
    |> cast(params, [:url, :schema, :username, :password, :hostname, :database, :port])
  end

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset) do
    changeset
    |> validate_format(:url, ~r/(?:postgres|postgresql)\:\/\/.+/)
    |> then(fn changeset ->
      url = get_change(changeset, :url)
      hostname = get_change(changeset, :hostname)

      if url == nil and hostname == nil do
        msg = "either connection url or separate connection credentials must be provided"

        changeset
        |> add_error(:url, msg)
        |> add_error(:hostname, msg)
      else
        changeset
      end
    end)
  end

  @doc """
  Executes either an Ecto.Query or an sql string on the Postgres backend.

  If an sql string is provided, one can also provide parameters to be passed.
  Parameter placeholders should correspond to Postgres format, i.e. `$#`

  ### Examples

  ```elixir
  iex> execute_query(souce_backend, from(s in "log_event_..."))
  {:ok, [%{...}]}
  iex> execute_query(backend, "select body from log_event_table")
  {:ok, [%{...}]}
  iex> execute_query(backend, {"select $1 as c from log_event_table", ["value"]})
  {:ok, [%{...}]}
  ```
  """
  @impl Logflare.Backends.Adaptor
  def execute_query(%Backend{} = backend, %Ecto.Query{} = query) do
    mod = PgRepo.create_repo(backend)

    result =
      query
      |> mod.all()
      |> Enum.map(&nested_map_update/1)

    {:ok, result}
  end

  def execute_query(%Backend{} = backend, query_string) when is_binary(query_string),
    do: execute_query(backend, {query_string, []})

  def execute_query(%Backend{} = backend, {query_string, params})
      when is_binary(query_string) and is_list(params) do
    mod = PgRepo.create_repo(backend)

    result = mod.query!(query_string, params)

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
  defdelegate create_repo(backend), to: PgRepo
  defdelegate table_name(source), to: PgRepo
  defdelegate create_log_events_table(source_backend), to: PgRepo
  defdelegate destroy_instance(backend, timeout \\ 5000), to: PgRepo
  defdelegate insert_log_event(source, backend, log_event), to: PgRepo
  defdelegate insert_log_events(source, backend, log_events), to: PgRepo

  # GenServer
  @impl GenServer
  def init({source, backend}) do
    with :ok <- Backends.register_backend_for_ingest_dispatch(source, backend) do
      # try create migration table, might fail
      create_log_events_table({source, backend})

      state = %__MODULE__{
        config: backend.config,
        backend: backend,
        source: source,
        pipeline_name: Backends.via_source(source, Pipeline, backend.id)
      }

      {:ok, _pipeline_pid} = Pipeline.start_link(state)
      {:ok, state}
    end
  end
end
