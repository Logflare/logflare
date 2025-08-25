defmodule Logflare.Backends.Adaptor.ClickhouseAdaptor do
  @moduledoc """
  ClickHouse backend adaptor that relies on the `:ch` library.
  """

  @behaviour Logflare.Backends.Adaptor

  use Supervisor
  use TypedStruct

  import Logflare.Utils.Guards

  require Logger

  alias __MODULE__.ConnectionManager
  alias __MODULE__.Pipeline
  alias __MODULE__.Provisioner
  alias __MODULE__.QueryTemplates
  alias Ecto.Changeset
  alias Logflare.Backends
  alias Logflare.Backends.Backend
  alias Logflare.Backends.SourceRegistry
  alias Logflare.LogEvent
  alias Logflare.Source

  @ingest_timeout 15_000
  @query_timeout 60_000

  typedstruct do
    field(:config, %{
      url: String.t(),
      username: String.t(),
      password: String.t(),
      database: String.t(),
      port: non_neg_integer(),
      pool_size: non_neg_integer()
    })

    field(:source, Source.t())
    field(:backend, Backend.t())
    field(:backend_token, String.t())
    field(:source_token, atom())
    field(:ingest_connection, tuple())
    field(:pipeline_name, tuple())
  end

  @type source_backend_tuple :: {Source.t(), Backend.t()}
  @type via_tuple :: {:via, Registry, {module(), {pos_integer(), {module(), pos_integer()}}}}

  defguardp is_via_tuple(value)
            when is_tuple(value) and elem(value, 0) == :via and elem(value, 1) == Registry and
                   is_tuple(elem(value, 2))

  defdelegate connection_pool_via(arg), to: ConnectionManager

  @doc false
  def child_spec(arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]}
    }
  end

  @doc false
  @impl Logflare.Backends.Adaptor
  @spec start_link(source_backend_tuple()) :: Supervisor.on_start()
  def start_link({%Source{}, %Backend{}} = args) do
    Supervisor.start_link(__MODULE__, args, name: adaptor_via(args))
  end

  @doc false
  @spec which_children(source_backend_tuple()) :: [
          {term() | :undefined, Supervisor.child() | :restarting, :worker | :supervisor,
           [module()] | :dynamic}
        ]
  def which_children({%Source{}, %Backend{}} = args) do
    args
    |> adaptor_via()
    |> Supervisor.which_children()
  end

  @doc false
  @impl Logflare.Backends.Adaptor
  def execute_query(_ident, _query), do: {:error, :not_implemented}

  @impl Logflare.Backends.Adaptor
  def supports_default_ingest?, do: true

  @doc false
  @impl Logflare.Backends.Adaptor
  def cast_config(%{} = params) do
    {%{},
     %{
       url: :string,
       username: :string,
       password: :string,
       database: :string,
       port: :integer,
       pool_size: :integer
     }}
    |> Changeset.cast(params, [
      :url,
      :username,
      :password,
      :database,
      :port,
      :pool_size
    ])
  end

  @doc false
  @impl Logflare.Backends.Adaptor
  def validate_config(%Changeset{} = changeset) do
    import Ecto.Changeset

    changeset
    |> validate_required([:url, :database, :port])
    |> Changeset.validate_format(:url, ~r/https?\:\/\/.+/)
    |> validate_user_pass()
  end

  @doc """
  Simple GRANT check to indicate if the configured user has the ClickHouse permissions it needs for the configured database.
  """
  @impl Logflare.Backends.Adaptor
  @spec test_connection({Source.t(), Backend.t()} | Backend.t()) ::
          :ok | {:error, :permissions_missing} | {:error, term()}
  def test_connection({%Source{} = source, %Backend{} = backend}) do
    sql_statement = QueryTemplates.grant_check_statement()

    case execute_ch_ingest_query({source, backend}, sql_statement) do
      {:ok, %Ch.Result{command: :check, rows: [[1]]}} ->
        :ok

      {:ok, %Ch.Result{command: :check, rows: [[0]]}} ->
        Logger.warning(
          "ClickHouse GRANT check failed. Required: `CREATE TABLE`, `ALTER TABLE`, `INSERT`, `SELECT`, `DROP TABLE`, `CREATE VIEW`, `DROP VIEW`",
          source_token: source.token,
          backend_id: backend.id
        )

        {:error, :permissions_missing}

      {:error, _} = error_result ->
        Logger.warning(
          "ClickHouse GRANT check failed. Unexpected error #{inspect(error_result)}",
          source_token: source.token,
          backend_id: backend.id
        )

        error_result
    end
  end

  def test_connection(%Backend{} = backend) do
    sql_statement = QueryTemplates.grant_check_statement()

    case execute_ch_read_query(backend, sql_statement) do
      {:ok, [%{"result" => 1}]} ->
        :ok

      {:ok, [%{"result" => 0}]} ->
        Logger.warning(
          "ClickHouse GRANT check failed. Required: `CREATE TABLE`, `ALTER TABLE`, `INSERT`, `SELECT`, `DROP TABLE`, `CREATE VIEW`, `DROP VIEW`",
          backend_id: backend.id
        )

        {:error, :permissions_missing}

      {:error, _} = error_result ->
        Logger.warning(
          "ClickHouse GRANT check failed. Unexpected error #{inspect(error_result)}",
          backend_id: backend.id
        )

        error_result
    end
  end

  @doc """
  Generates a via tuple based on a `Source` and `Backend` pair for this adaptor instance.

  See `Backends.via_source/3` for more details.
  """
  @spec adaptor_via(source_backend_tuple()) :: via_tuple()
  def adaptor_via({%Source{} = source, %Backend{} = backend}) do
    Backends.via_source(source, __MODULE__, backend)
  end

  @doc """
  Generates a unique Broadway pipeline via tuple based on a `Source` and `Backend` pair.

  See `Backends.via_source/3` for more details.
  """
  @spec pipeline_via(source_backend_tuple()) :: via_tuple()
  def pipeline_via({%Source{} = source, %Backend{} = backend}) do
    Backends.via_source(source, Pipeline, backend)
  end

  @doc """
  Returns the pid for the Broadway pipeline related to a specific `Source` and `Backend` pair.

  If the process is not located in the registry or does not exist, this will return `nil`.
  """
  @spec pipeline_pid(source_backend_tuple()) :: pid() | nil
  def pipeline_pid({%Source{}, %Backend{}} = args) do
    case find_pipeline_pid(args) do
      {:ok, pid} -> pid
      _ -> nil
    end
  end

  @doc """
  Determines if a particular Broadway pipeline process is alive based on a `Source` and `Backend` pair.
  """
  @spec pipeline_alive?(source_backend_tuple()) :: boolean()
  def pipeline_alive?({%Source{}, %Backend{}} = args) do
    case pipeline_pid(args) do
      nil -> false
      pid -> Process.alive?(pid)
    end
  end

  @doc """
  Produces a unique ingest table name for ClickHouse based on a provided `Source` struct.
  """
  @spec clickhouse_ingest_table_name(Source.t()) :: String.t()
  def clickhouse_ingest_table_name(%Source{} = source) do
    source
    |> clickhouse_source_token()
    |> then(&"#{QueryTemplates.default_table_name_prefix()}_#{&1}")
    |> check_clickhouse_resource_name_length(source)
  end

  @doc """
  Produces a unique key count table name for ClickHouse based on a provided `Source` struct.
  """
  @spec clickhouse_key_count_table_name(Source.t()) :: String.t()
  def clickhouse_key_count_table_name(%Source{} = source) do
    source
    |> clickhouse_source_token()
    |> then(&"#{QueryTemplates.default_key_type_counts_table_prefix()}_#{&1}")
    |> check_clickhouse_resource_name_length(source)
  end

  @doc """
  Produces a unique materialized view name for ClickHouse based on a provided `Source` struct.
  """
  @spec clickhouse_materialized_view_name(Source.t()) :: String.t()
  def clickhouse_materialized_view_name(%Source{} = source) do
    source
    |> clickhouse_source_token()
    |> then(&"#{QueryTemplates.default_key_type_counts_view_prefix()}_#{&1}")
    |> check_clickhouse_resource_name_length(source)
  end

  @doc """
  Executes a raw ClickHouse query using the ingest connection pool.

  This function is for write operations like inserts, DDL statements, and provisioning.
  """
  @spec execute_ch_ingest_query(
          source_backend_tuple(),
          statement :: iodata(),
          params :: map | [term] | [row :: [term]] | iodata | Enumerable.t(),
          [Ch.query_option()]
        ) :: {:ok, Ch.Result.t()} | {:error, Exception.t()}
  def execute_ch_ingest_query(source_backend, statement, params \\ [], opts \\ [])

  def execute_ch_ingest_query(
        {%Source{}, %Backend{}} = source_backend,
        statement,
        params,
        opts
      )
      when is_list(params) and is_list(opts) do
    ConnectionManager.ensure_pool_started(source_backend)
    ConnectionManager.notify_activity(source_backend)

    source_backend
    |> connection_pool_via()
    |> Ch.query(statement, params, opts)
  end

  @doc """
  Executes a raw ClickHouse query using the query connection pool.

  This function is for read operations like SELECT queries and analytics.
  """
  @spec execute_ch_read_query(
          Backend.t(),
          statement :: iodata(),
          params :: map | [term] | [row :: [term]] | iodata | Enumerable.t(),
          [Ch.query_option()]
        ) :: {:ok, Ch.Result.t()} | {:error, Exception.t()}
  def execute_ch_read_query(backend, statement, params \\ [], opts \\ [])

  def execute_ch_read_query(%Backend{} = backend, statement, params, opts)
      when is_list(params) and is_list(opts) do
    ConnectionManager.ensure_pool_started(backend)
    ConnectionManager.notify_activity(backend)

    pool_via = connection_pool_via(backend)

    case Ch.query(pool_via, statement, params, opts) do
      {:ok, %Ch.Result{} = result} ->
        {:ok, convert_ch_result_to_rows(result)}

      {:error, %Ch.Error{message: error_msg}} when is_non_empty_binary(error_msg) ->
        Logger.warning(
          "ClickHouse query failed: #{inspect(error_msg)}",
          backend_id: backend.id
        )

        {:error, "Error executing Clickhouse query"}

      {:error, reason} when is_non_empty_binary(reason) ->
        Logger.warning(
          "ClickHouse query failed: #{inspect(reason)}",
          backend_id: backend.id
        )

        {:error, "Error executing Clickhouse query"}

      {:error, _} ->
        {:error, "Error executing Clickhouse query"}
    end
  end

  @doc """
  Inserts a single `LogEvent` struct into the given source backend table.

  See `insert_log_events/2` and `insert_log_events/3` for additional details.
  """
  @spec insert_log_event(source_backend_tuple(), LogEvent.t()) ::
          {:ok, Ch.Result.t()} | {:error, Exception.t()}
  def insert_log_event({%Source{} = source, %Backend{} = backend}, %LogEvent{} = le),
    do: insert_log_events({source, backend}, [le])

  @spec insert_log_event(via_tuple(), Backend.t(), LogEvent.t()) ::
          {:ok, Ch.Result.t()} | {:error, Exception.t()}
  def insert_log_event(conn_via, %Backend{} = backend, %LogEvent{} = le)
      when is_via_tuple(conn_via),
      do: insert_log_events(conn_via, backend, [le])

  @doc """
  Inserts a list of `LogEvent` structs into a given source backend table.

  Supports either a `{Source, Backend}` tuple or a via tuple to determine the ClickHouse connection to use.

  See `execute_ch_query/4` for additional details.
  """
  @spec insert_log_events(source_backend_tuple(), [LogEvent.t()]) ::
          {:ok, Ch.Result.t()} | {:error, Exception.t()}
  def insert_log_events({%Source{}, %Backend{}} = source_backend, events) when is_list(events) do
    source_backend
    |> connection_pool_via()
    |> insert_log_events(source_backend, events)
  end

  @spec insert_log_events(via_tuple(), source_backend_tuple(), [LogEvent.t()]) ::
          {:ok, Ch.Result.t()} | {:error, Exception.t()}
  def insert_log_events(conn_via, {%Source{} = source, _backend}, events)
      when is_via_tuple(conn_via) and is_list(events) do
    table_name = clickhouse_ingest_table_name(source)

    event_params =
      Enum.map(events, fn %LogEvent{} = log_event ->
        flattened_body =
          log_event.body
          |> Map.drop(["id", "event_message", "timestamp"])
          |> Iteraptor.to_flatmap()

        [
          log_event.body["id"],
          log_event.body["event_message"],
          Jason.encode!(flattened_body),
          DateTime.from_unix!(log_event.body["timestamp"], :microsecond)
        ]
      end)

    opts = [
      names: ["id", "event_message", "body", "timestamp"],
      types: ["UUID", "String", "String", "DateTime64(6)"]
    ]

    Ch.query(
      conn_via,
      "INSERT INTO #{table_name} FORMAT RowBinaryWithNamesAndTypes",
      event_params,
      opts
    )
  end

  @doc """
  Attempts to provision a new log ingest table, if it does not already exist.
  """
  @spec provision_ingest_table(source_backend_tuple()) ::
          {:ok, Ch.Result.t()} | {:error, Exception.t()}
  def provision_ingest_table({%Source{} = source, %Backend{}} = args) do
    with table_name <- clickhouse_ingest_table_name(source),
         statement <-
           QueryTemplates.create_log_ingest_table_statement(table_name,
             ttl_days: source.retention_days
           ) do
      execute_ch_ingest_query(args, statement)
    end
  end

  @doc """
  Attempts to provision a new key type counts table, if it does not already exist.
  """
  @spec provision_key_type_counts_table(source_backend_tuple()) ::
          {:ok, Ch.Result.t()} | {:error, Exception.t()}
  def provision_key_type_counts_table({%Source{} = source, %Backend{}} = args) do
    with key_count_table_name <- clickhouse_key_count_table_name(source),
         statement <-
           QueryTemplates.create_key_type_counts_table_statement(table: key_count_table_name) do
      execute_ch_ingest_query(args, statement)
    end
  end

  @doc """
  Attempts to provision a new materialized view, if it does not already exist.
  """
  @spec provision_materialized_view(source_backend_tuple()) ::
          {:ok, Ch.Result.t()} | {:error, Exception.t()}
  def provision_materialized_view({%Source{} = source, %Backend{}} = args) do
    with source_table_name <- clickhouse_ingest_table_name(source),
         view_name <- clickhouse_materialized_view_name(source),
         key_count_table_name <- clickhouse_key_count_table_name(source),
         statement <-
           QueryTemplates.create_materialized_view_statement(source_table_name,
             view_name: view_name,
             key_table: key_count_table_name
           ) do
      execute_ch_ingest_query(args, statement)
    end
  end

  @doc """
  Handles all provisioning tasks for a given `Source` and `Backend` pair.
  """
  @spec provision_all(source_backend_tuple()) :: :ok | {:error, term()}
  def provision_all({%Source{}, %Backend{}} = args) do
    with {:ok, _} <- provision_ingest_table(args),
         {:ok, _} <- provision_key_type_counts_table(args),
         {:ok, _} <- provision_materialized_view(args) do
      :ok
    end
  end

  @doc false
  @impl Supervisor
  def init({%Source{} = source, %Backend{config: %{} = config} = backend} = args) do
    default_pool_size = Application.fetch_env!(:logflare, :clickhouse_backend_adaptor)[:pool_size]
    ingest_pool_size = Map.get(config, :pool_size, default_pool_size)

    # set the query pool size to half of the write pool size, if larger than the default
    query_pool_size =
      ingest_pool_size
      |> div(2)
      |> max(default_pool_size)

    url = Map.get(config, :url)
    {:ok, {scheme, hostname}} = extract_scheme_and_hostname(url)

    ingest_pool_via = connection_pool_via({source, backend})
    query_pool_via = connection_pool_via(backend)

    pipeline_state = %__MODULE__{
      config: config,
      backend: backend,
      backend_token: backend.token,
      source_token: source.token,
      source: source,
      ingest_connection: ingest_pool_via,
      pipeline_name: pipeline_via({source, backend})
    }

    # Ingest connection pool opts
    ingest_ch_opts = [
      name: ingest_pool_via,
      scheme: scheme,
      hostname: hostname,
      port: get_port_config(backend),
      database: Map.get(config, :database),
      username: Map.get(config, :username),
      password: Map.get(config, :password),
      pool_size: ingest_pool_size,
      settings: [],
      timeout: @ingest_timeout
    ]

    # Query (reads) connection pool opts
    query_ch_opts = [
      name: query_pool_via,
      scheme: scheme,
      hostname: hostname,
      port: get_port_config(backend),
      database: Map.get(config, :database),
      username: Map.get(config, :username),
      password: Map.get(config, :password),
      pool_size: query_pool_size,
      settings: [],
      timeout: @query_timeout
    ]

    children = [
      ConnectionManager.child_spec({source, backend, ingest_ch_opts}),
      ConnectionManager.child_spec({backend, query_ch_opts}),
      Provisioner.child_spec(args),
      Pipeline.child_spec(pipeline_state)
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @spec find_pipeline_pid(source_backend_tuple()) :: {:ok, pid()} | {:error, term()}
  defp find_pipeline_pid({%Source{}, %Backend{}} = args) do
    args
    |> pipeline_via()
    |> find_pid_in_source_registry()
  end

  @spec find_pid_in_source_registry(via_tuple()) :: {:ok, pid()} | {:error, term()}
  defp find_pid_in_source_registry({:via, Registry, {SourceRegistry, key}}) do
    case Registry.lookup(SourceRegistry, key) do
      [{pid, _meta}] ->
        {:ok, pid}

      _ ->
        {:error, :not_found}
    end
  end

  @spec extract_scheme_and_hostname(String.t()) ::
          {:ok, {String.t(), String.t()}} | {:error, String.t()}
  defp extract_scheme_and_hostname(url) when is_non_empty_binary(url) do
    case URI.new(url) do
      {:ok, %URI{scheme: scheme, host: hostname}} when scheme in ~w(http https) ->
        {:ok, {scheme, hostname}}

      {:ok, %URI{}} ->
        {:error, "Unable to extract scheme and hostname from URL '#{inspect(url)}'."}

      {:error, _err_msg} = error ->
        error
    end
  end

  defp extract_scheme_and_hostname(_url), do: {:error, "Unexpected URL value provided."}

  @spec clickhouse_source_token(Source.t()) :: String.t()
  defp clickhouse_source_token(%Source{token: token}) do
    token
    |> Atom.to_string()
    |> String.replace("-", "_")
  end

  @spec check_clickhouse_resource_name_length(name :: String.t(), source :: Source.t()) ::
          String.t()
  defp check_clickhouse_resource_name_length(name, %Source{} = source)
       when is_non_empty_binary(name) do
    if String.length(name) >= 200 do
      resource_prefix = String.slice(name, 0, 40) <> "..."

      raise "The dynamically generated ClickHouse resource name starting with `#{resource_prefix}` exceeds the maximum limit. Source ID: #{source.id}"
    else
      name
    end
  end

  @spec get_port_config(Backend.t()) :: non_neg_integer()
  defp get_port_config(%Backend{config: %{port: port}}) when is_pos_integer(port), do: port

  defp get_port_config(%Backend{config: %{port: port}}) when is_non_empty_binary(port),
    do: String.to_integer(port)

  defp validate_user_pass(changeset) do
    user = Changeset.get_field(changeset, :username)
    pass = Changeset.get_field(changeset, :password)
    user_pass = [user, pass]

    if user_pass != [nil, nil] and Enum.any?(user_pass, &is_nil/1) do
      msg = "Both username and password must be provided for auth"

      changeset
      |> Changeset.add_error(:username, msg)
      |> Changeset.add_error(:password, msg)
    else
      changeset
    end
  end

  @spec convert_ch_result_to_rows(Ch.Result.t()) :: [map()]
  defp convert_ch_result_to_rows(%Ch.Result{} = result) do
    case {result.columns, result.rows} do
      {nil, nil} ->
        []

      {nil, rows} when is_list(rows) ->
        # No column names, return rows as-is
        rows

      {_columns, nil} ->
        # No rows
        []

      {columns, rows} when is_list(columns) and is_list(rows) ->
        # Convert rows to maps using column names
        for row <- rows do
          columns
          |> Enum.zip(row)
          |> Map.new()
        end

      {columns, rows} ->
        # Handle other formats - Ch.Result.rows can be iodata
        Logger.warning(
          "Unexpected ClickHouse result format: columns=#{inspect(columns)}, rows=#{inspect(rows)}"
        )

        []
    end
  end
end
