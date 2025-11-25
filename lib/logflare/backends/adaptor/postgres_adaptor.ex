defmodule Logflare.Backends.Adaptor.PostgresAdaptor do
  @moduledoc """
  The backend adaptor for the Postgres database.

  Config:
  `:url` - the database connection string

  On ingest, pipeline will insert it into the log event table for the given source.
  """

  @behaviour Logflare.Backends.Adaptor

  use GenServer
  use TypedStruct

  import Ecto.Changeset
  import Logflare.Utils.Guards

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.PostgresAdaptor.PgRepo
  alias Logflare.Backends.Adaptor.PostgresAdaptor.Pipeline
  alias Logflare.Backends.Adaptor.PostgresAdaptor.SharedRepo
  alias Logflare.Backends.Backend
  alias Logflare.Backends.Ecto.SqlUtils
  alias Logflare.SingleTenant
  alias Logflare.Sources.Source
  alias Logflare.Sql

  require Logger

  typedstruct do
    field(:config, %{
      url: String.t(),
      schema: String.t(),
      username: String.t(),
      password: String.t(),
      hostname: String.t(),
      port: non_neg_integer(),
      pool_size: non_neg_integer()
    })

    field(:source, Source.t())
    field(:backend, Backend.t())
    field(:backend_token, String.t())
    field(:source_token, atom())
    field(:pipeline_name, tuple())
  end

  @impl Logflare.Backends.Adaptor
  def start_link({source, backend}) do
    GenServer.start_link(__MODULE__, {source, backend},
      name: Backends.via_source(source, __MODULE__, backend.id)
    )
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
       port: :integer,
       pool_size: :integer
     }}
    |> cast(params, [:url, :schema, :username, :password, :hostname, :database, :port, :pool_size])
  end

  @doc """
  Executes either an `Ecto.Query` or a sql string against the Postgres backend.

  If a sql string is provided, one can also provide parameters to be passed.
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
  def execute_query(%Backend{} = backend, %Ecto.Query{} = query, _opts) do
    mod = PgRepo.create_repo(backend)

    result =
      query
      |> mod.all()
      |> Enum.map(&nested_map_update/1)

    {:ok, result}
  end

  def execute_query(%Backend{} = backend, query_string, opts)
      when is_non_empty_binary(query_string) and is_list(opts) do
    execute_query(backend, {query_string, []}, opts)
  end

  def execute_query(%Backend{} = backend, {query_string, params}, _opts)
      when is_non_empty_binary(query_string) and is_list(params) do
    {:ok, result} =
      SharedRepo.with_repo(backend, fn ->
        SharedRepo.query(query_string, params)
      end)

    rows =
      for row <- result.rows do
        result.columns
        |> Enum.zip(row)
        |> Map.new()
        |> nested_map_update()
      end

    {:ok, rows}
  end

  def execute_query(%Backend{} = backend, {query_string, declared_params, input_params}, opts)
      when is_non_empty_binary(query_string) and is_list(declared_params) and is_map(input_params) and
             is_list(opts) do
    args = Enum.map(declared_params, fn param -> Map.get(input_params, param) end)
    execute_query(backend, {query_string, args}, opts)
  end

  def execute_query(
        %Backend{} = backend,
        {query_string, declared_params, input_params, _endpoint_query},
        opts
      )
      when is_non_empty_binary(query_string) and is_list(declared_params) and is_map(input_params) and
             is_list(opts) do
    execute_query(backend, {query_string, declared_params, input_params}, opts)
  end

  @impl Logflare.Backends.Adaptor
  def ecto_to_sql(%Ecto.Query{} = query, _opts) do
    SqlUtils.ecto_to_pg_sql(query)
  end

  @impl Logflare.Backends.Adaptor
  def map_query_parameters(original_query, _transformed_query, _declared_params, input_params) do
    {:ok, params} = Sql.parameter_positions(original_query)

    params
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(fn {_pos, parameter} ->
      Map.get(input_params, parameter)
    end)
  end

  @impl Logflare.Backends.Adaptor
  def supports_default_ingest?, do: true

  @impl Logflare.Backends.Adaptor
  def transform_query(query, :bq_sql, context) do
    if should_transform_bigquery_to_postgres?() do
      schema_prefix = Map.get(context, :schema_prefix)
      Sql.translate(:bq_sql, :pg_sql, query, schema_prefix)
    else
      {:error, "BigQuery SQL not supported by this PostgreSQL backend"}
    end
  end

  def transform_query(query, :pg_sql, _context) do
    {:ok, query}
  end

  def transform_query(_query, from_language, _context) do
    {:error, "Transformation from #{from_language} to PostgreSQL not supported"}
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

  @impl Logflare.Backends.Adaptor
  def redact_config(config) do
    url = Map.get(config, :url) || Map.get(config, "url")
    updated = String.replace(url, ~r/(.+):.+\@/, "\\g{1}:REDACTED@")
    Map.put(config, :url, updated)
  end

  # expose PgRepo functions
  defdelegate create_repo(backend), to: PgRepo
  defdelegate table_name(source), to: PgRepo
  defdelegate create_events_table(source_backend), to: PgRepo
  defdelegate destroy_instance(backend, timeout \\ 5000), to: PgRepo
  defdelegate insert_log_event(source, backend, log_event), to: PgRepo
  defdelegate insert_log_events(source, backend, log_events), to: PgRepo

  # GenServer
  @impl GenServer
  def init({source, backend}) do
    # try create migration table, might fail
    res = create_events_table({source, backend})

    if :ok != res do
      Logger.warning("Failed to create events table: #{inspect(res)} ",
        source_token: source.token,
        backend_id: backend.id
      )
    end

    state = %__MODULE__{
      config: backend.config,
      backend: backend,
      backend_token: if(backend, do: backend.token, else: nil),
      source_token: source.token,
      source: source,
      pipeline_name: Backends.via_source(source, Pipeline, backend.id)
    }

    {:ok, _pipeline_pid} = Pipeline.start_link(state)
    {:ok, state}
  end

  @spec should_transform_bigquery_to_postgres?() :: boolean()
  defp should_transform_bigquery_to_postgres?() do
    # Only transform BigQuery SQL -> PostgreSQL in SingleTenant Supabase mode with PostgreSQL backend enabled
    SingleTenant.supabase_mode?() and SingleTenant.postgres_backend?()
  end

  @spec nested_map_update(term()) :: term()
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
end
