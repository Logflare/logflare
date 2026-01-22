defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor do
  @moduledoc """
  ClickHouse backend adaptor that relies on the `:ch` library.

  This adaptor uses consolidated ingestion where all sources share a single
  pipeline and table per backend.
  """

  @behaviour Logflare.Backends.Adaptor

  use Supervisor

  import Logflare.Utils.Guards

  require Logger

  alias __MODULE__.ConnectionManager
  alias __MODULE__.Ingester
  alias __MODULE__.Pipeline
  alias __MODULE__.Provisioner
  alias __MODULE__.QueryTemplates
  alias Ecto.Changeset
  alias Logflare.Backends
  alias Logflare.Backends.Backend
  alias Logflare.Backends.DynamicPipeline
  alias Logflare.Backends.Ecto.SqlUtils
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.LogEvent

  @min_pipelines 1
  @resolve_interval 10_000
  @scaling_threshold 5_000

  defdelegate connection_pool_via(arg), to: ConnectionManager

  @doc false
  def child_spec(arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]}
    }
  end

  @impl Logflare.Backends.Adaptor
  def consolidated_ingest?, do: true

  @doc false
  @impl Logflare.Backends.Adaptor
  @spec start_link(Backend.t()) :: Supervisor.on_start()
  def start_link(%Backend{} = backend) do
    Supervisor.start_link(__MODULE__, backend, name: Backends.via_backend(backend, __MODULE__))
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
    case execute_ch_query(backend, query_string, params) do
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
  @spec test_connection(Backend.t()) :: :ok | {:error, :permissions_missing} | {:error, term()}
  def test_connection(%Backend{} = backend) do
    sql_statement = QueryTemplates.grant_check_statement()

    case execute_ch_query(backend, sql_statement) do
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
  Produces a unique ingest table name for ClickHouse based on a provided `Backend` struct.

  This table holds events from ALL sources using this backend.
  """
  @spec clickhouse_ingest_table_name(Backend.t()) :: String.t()
  def clickhouse_ingest_table_name(%Backend{token: token}) do
    token_str = String.replace(token, "-", "_")
    table_name = "#{QueryTemplates.default_table_name_prefix()}_#{token_str}"

    if String.length(table_name) >= 200 do
      raise "The dynamically generated ClickHouse resource name starting with `ingest_` " <>
              "must be less than 200 characters. Got: #{String.length(table_name)}"
    end

    table_name
  end

  @doc """
  Executes a raw ClickHouse query using the query connection pool.

  This function is for read operations like SELECT queries and analytics.
  """
  @spec execute_ch_query(
          Backend.t(),
          statement :: iodata(),
          params :: map | [term] | [row :: [term]] | iodata | Enumerable.t(),
          [Ch.query_option()]
        ) :: {:ok, Ch.Result.t()} | {:error, Exception.t()}
  def execute_ch_query(backend, statement, params \\ [], opts \\ [])

  def execute_ch_query(%Backend{} = backend, statement, params, opts)
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

          {:error, "Error executing ClickHouse query"}

        {:error, %{message: message}} when is_non_empty_binary(message) ->
          Logger.warning(
            "ClickHouse query failed: #{inspect(message)}",
            backend_id: backend.id
          )

          {:error, "Error executing ClickHouse query"}

        {:error, _} ->
          {:error, "Error executing ClickHouse query"}
      end
    end
  end

  @doc """
  Inserts a list of `LogEvent` structs into the backend's ingest table.
  """
  @spec insert_log_events(Backend.t(), [LogEvent.t()]) :: :ok | {:error, String.t()}
  def insert_log_events(%Backend{}, []), do: :ok

  def insert_log_events(%Backend{} = backend, [%LogEvent{} | _] = events) do
    Logger.metadata(backend_id: backend.id)

    table_name = clickhouse_ingest_table_name(backend)

    case Ingester.insert(backend, table_name, events) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("ClickHouse insert errors.", error_string: inspect(reason))

        {:error, reason}
    end
  end

  @doc """
  Provisions a new ingest table for the backend, if it does not already exist.
  """
  @spec provision_ingest_table(Backend.t()) :: {:ok, [map()]} | {:error, Exception.t()}
  def provision_ingest_table(%Backend{} = backend) do
    table_name = clickhouse_ingest_table_name(backend)
    statement = QueryTemplates.create_ingest_table_statement(table_name)
    execute_ch_query(backend, statement)
  end

  @doc false
  @impl Supervisor
  def init(%Backend{} = backend) do
    children = [
      Provisioner.child_spec(backend),
      {
        DynamicPipeline,
        name: Backends.via_backend(backend, Pipeline),
        pipeline: Pipeline,
        pipeline_args: [backend: backend],
        min_pipelines: @min_pipelines,
        max_pipelines: System.schedulers_online(),
        initial_count: @min_pipelines,
        resolve_interval: @resolve_interval,
        resolve_count: fn state ->
          lens = IngestEventQueue.list_pending_counts({:consolidated, backend.id})

          resolve_pipeline_count(state, lens)
        end
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  # produce fewer, larger batches for ClickHouse efficiency
  @spec resolve_pipeline_count(map(), [{term(), non_neg_integer()}]) :: non_neg_integer()
  defp resolve_pipeline_count(state, lens) do
    startup_size =
      Enum.find_value(lens, 0, fn
        {{:consolidated, _bid, nil}, val} -> val
        _ -> false
      end)

    lens_no_startup =
      Enum.filter(lens, fn
        {{:consolidated, _bid, nil}, _val} -> false
        _ -> true
      end)

    lens_no_startup_values = Enum.map(lens_no_startup, fn {_, v} -> v end)
    len = Enum.map(lens, fn {_, v} -> v end) |> Enum.sum()

    last_decr = state.last_count_decrease || NaiveDateTime.utc_now()
    sec_since_last_decr = NaiveDateTime.diff(NaiveDateTime.utc_now(), last_decr)

    # Higher threshold (5,000) to allow more buffering before scaling
    any_above_threshold? = Enum.any?(lens_no_startup_values, &(&1 >= @scaling_threshold))

    cond do
      # Scale up if startup queue has events (pipeline not yet ready)
      startup_size > 0 ->
        state.pipeline_count + ceil(startup_size / @scaling_threshold)

      # Scale up if any queue exceeds threshold
      any_above_threshold? and len > 0 ->
        state.pipeline_count + ceil(len / @scaling_threshold)

      # Gradual decrease when queues are low
      Enum.all?(lens_no_startup_values, &(&1 < div(@scaling_threshold, 10))) and
        len < @scaling_threshold and state.pipeline_count > 1 and
          (sec_since_last_decr > 60 or state.last_count_decrease == nil) ->
        state.pipeline_count - 1

      true ->
        state.pipeline_count
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
        convert_uuids(rows)

      {_columns, nil} ->
        []

      {columns, rows} when is_list(columns) and is_list(rows) ->
        for row <- rows do
          columns
          |> Enum.zip(row)
          |> Map.new()
        end
        |> convert_uuids()

      {columns, rows} ->
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

    case execute_ch_query(backend, converted_query, ch_params) do
      {:ok, result} -> {:ok, result}
      error -> error
    end
  end

  @spec convert_query_params(sql_statement :: String.t(), allowed_params :: [String.t()]) ::
          String.t()
  defp convert_query_params(sql_statement, allowed_params)
       when is_non_empty_binary(sql_statement) and is_list(allowed_params) do
    allowed_set = MapSet.new(allowed_params)

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
