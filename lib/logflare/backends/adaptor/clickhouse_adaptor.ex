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
  alias Logflare.Backends.DynamicPipeline
  alias Logflare.Backends.Backend
  alias Logflare.Backends.Ecto.SqlUtils
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.SourceRegistry
  alias Logflare.LogEvent
  alias Logflare.Sources.Source
  alias Logflare.Sources

  typedstruct do
    field(:source, Source.t())
    field(:backend, Backend.t())
    field(:ingest_connection, tuple())
  end

  @type source_backend_tuple :: {Source.t(), Backend.t()}
  @type via_tuple :: {:via, Registry, {module(), {pos_integer(), {module(), pos_integer()}}}}

  defdelegate connection_pool_via(arg), to: ConnectionManager

  defguardp is_list_or_map(value) when is_list(value) or is_map(value)

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

  @impl Logflare.Backends.Adaptor
  def ecto_to_sql(%Ecto.Query{} = query, opts) do
    case Logflare.Ecto.ClickHouse.to_sql(query, opts) do
      {:ok, {ch_sql, ch_params}} ->
        ch_params = Enum.map(ch_params, &SqlUtils.normalize_datetime_param/1)
        {:ok, {ch_sql, ch_params}}

      {:error, _reason} = error ->
        error
    end
  end

  @impl Logflare.Backends.Adaptor
  def redact_config(config) do
    Map.put(config, :password, "REDACTED")
  end

  @doc false
  @impl Logflare.Backends.Adaptor
  def execute_query(%Backend{} = backend, query_string, opts)
      when is_non_empty_binary(query_string) and is_list(opts) do
    execute_query(backend, {query_string, []}, opts)
  end

  def execute_query(%Backend{} = backend, {query_string, params}, _opts)
      when is_non_empty_binary(query_string) and is_list(params) do
    case execute_ch_read_query(backend, query_string, params) do
      {:ok, result} -> {:ok, result}
      error -> error
    end
  end

  def execute_query(%Backend{} = backend, {query_string, declared_params, input_params}, _opts)
      when is_non_empty_binary(query_string) and is_list(declared_params) and is_map(input_params) do
    execute_query_with_params(backend, query_string, declared_params, input_params)
  end

  def execute_query(
        %Backend{} = backend,
        {query_string, declared_params, input_params, _endpoint_query},
        _opts
      )
      when is_non_empty_binary(query_string) and is_list(declared_params) and is_map(input_params) do
    execute_query_with_params(backend, query_string, declared_params, input_params)
  end

  def execute_query(%Backend{} = backend, %Ecto.Query{} = query, opts) when is_list(opts) do
    with {:ok, {ch_sql, ch_params}} <- ecto_to_sql(query, opts) do
      execute_query(backend, {ch_sql, ch_params}, opts)
    end
  end

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

  def execute_ch_ingest_query({%Source{}, %Backend{}} = source_backend, statement, params, opts)
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
      when is_list_or_map(params) and is_list(opts) do
    with :ok <- ensure_query_connection_manager_started(backend) do
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

        {:error, %{message: message}} when is_non_empty_binary(message) ->
          Logger.warning(
            "ClickHouse query failed: #{inspect(message)}",
            backend_id: backend.id
          )

          {:error, "Error executing Clickhouse query"}

        {:error, _} ->
          {:error, "Error executing Clickhouse query"}
      end
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

  @doc """
  Inserts a list of `LogEvent` structs into a given source backend table.

  See `execute_ch_query/4` for additional details.
  """
  @spec insert_log_events(source_backend_tuple(), [LogEvent.t()]) ::
          {:ok, Ch.Result.t()} | {:error, Exception.t()}
  def insert_log_events({%Source{} = source, %Backend{}} = source_backend, events)
      when is_list(events) do
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

    execute_ch_ingest_query(
      source_backend,
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
  def init({%Source{} = source, %Backend{} = backend} = args) do
    children = [
      ConnectionManager.child_spec({source, backend}),
      Provisioner.child_spec(args),
      {
        DynamicPipeline,
        # soft limit before a new pipeline is created
        name: Backends.via_source(source, Pipeline, backend.id),
        pipeline: Pipeline,
        pipeline_args: [
          source: source,
          backend: backend
        ],
        min_pipelines: 0,
        max_pipelines: System.schedulers_online(),
        initial_count: 1,
        resolve_interval: 2_500,
        resolve_count: fn state ->
          source = Sources.refresh_source_metrics_for_ingest(source)

          lens = IngestEventQueue.list_pending_counts({source.id, backend.id})

          Backends.handle_resolve_count(state, lens, source.metrics.avg)
        end
      }
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
        convert_uuids(rows)

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
        |> convert_uuids()

      {columns, rows} ->
        # Handle other formats - Ch.Result.rows can be iodata
        Logger.warning(
          "Unexpected ClickHouse result format: columns=#{inspect(columns)}, rows=#{inspect(rows)}"
        )

        []
    end
  end

  @spec execute_query_with_params(
          Backend.t(),
          query_string :: String.t(),
          declared_params :: [String.t()],
          input_params :: map()
        ) ::
          {:ok, [map()]} | {:error, any()}
  defp execute_query_with_params(
         %Backend{} = backend,
         query_string,
         declared_params,
         input_params
       ) do
    converted_query = convert_query_params(query_string, declared_params)
    ch_params = Map.take(input_params, declared_params)

    case execute_ch_read_query(backend, converted_query, ch_params) do
      {:ok, result} -> {:ok, result}
      error -> error
    end
  end

  @spec convert_query_params(sql_statement :: String.t(), allowed_params :: [String.t()]) ::
          String.t()
  defp convert_query_params(sql_statement, allowed_params)
       when is_non_empty_binary(sql_statement) and is_list(allowed_params) do
    allowed_set = MapSet.new(allowed_params)

    # Convert `@param` syntax to ClickHouse `{param:String}` syntax
    Regex.replace(~r/@(\w+)/, sql_statement, fn match, param ->
      if MapSet.member?(allowed_set, param) do
        "{#{param}:String}"
      else
        match
      end
    end)
  end

  @spec convert_uuids(data :: any()) :: any()
  defp convert_uuids(data) when is_struct(data), do: data

  defp convert_uuids(data) when is_map(data) do
    Map.new(data, fn {k, v} -> {k, convert_uuids(v)} end)
  end

  defp convert_uuids(data) when is_list(data) do
    Enum.map(data, &convert_uuids/1)
  end

  defp convert_uuids(data) when is_non_empty_binary(data) and byte_size(data) == 16 do
    Ecto.UUID.cast!(data)
  end

  defp convert_uuids(data), do: data

  @spec ensure_query_connection_manager_started(Backend.t()) :: :ok | {:error, term()}
  defp ensure_query_connection_manager_started(%Backend{id: backend_id} = backend) do
    via = Backends.via_backend(backend, ConnectionManager)

    via
    |> GenServer.whereis()
    |> maybe_start_query_connection_manager(backend_id)
    |> case do
      :ok -> ensure_pool_and_notify(backend)
      error -> error
    end
  end

  @spec maybe_start_query_connection_manager(pid() | nil, pos_integer()) :: :ok | {:error, term()}
  defp maybe_start_query_connection_manager(nil, backend_id) when is_integer(backend_id) do
    # Fetch fresh backend from cache
    backend = Backends.Cache.get_backend(backend_id)

    with child_spec <- ConnectionManager.child_spec(backend),
         {:ok, _pid} <- __MODULE__.QueryConnectionSup.start_connection_manager(child_spec) do
      Logger.info(
        "Started query ConnectionManager for ClickHouse backend",
        backend_id: backend.id
      )

      :ok
    else
      {:error, {:already_started, _pid}} ->
        # Race condition / another process started it
        :ok

      {:error, reason} = error ->
        Logger.warning(
          "Failed to start query ConnectionManager for backend",
          backend_id: backend_id,
          reason: reason
        )

        error
    end
  end

  defp maybe_start_query_connection_manager(_pid, _backend_id), do: :ok

  @spec ensure_pool_and_notify(Backend.t()) :: :ok
  defp ensure_pool_and_notify(%Backend{} = backend) do
    ConnectionManager.ensure_pool_started(backend)
    ConnectionManager.notify_activity(backend)
    :ok
  end
end
