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
  alias __MODULE__.NativeIngester
  alias __MODULE__.NativeIngester.PoolSup, as: NativePoolSup
  alias __MODULE__.Pipeline
  alias __MODULE__.Provisioner
  alias __MODULE__.QueryTemplates
  alias Ecto.Changeset
  alias Logflare.Backends
  alias Logflare.Ecto.ClickHouse, as: EctoClickHouse
  alias Logflare.Backends.Backend
  alias Logflare.Backends.DynamicPipeline
  alias Logflare.Backends.Ecto.SqlUtils
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.LogEvent
  alias Logflare.LogEvent.TypeDetection

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
    case EctoClickHouse.to_sql(query, opts) do
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
    {%{insert_protocol: "http"},
     %{
       url: :string,
       username: :string,
       password: :string,
       database: :string,
       port: :integer,
       pool_size: :integer,
       async_insert: :boolean,
       read_only_url: :string,
       insert_protocol: :string,
       native_port: :integer,
       native_pool_size: :integer
     }}
    |> Changeset.cast(params, [
      :url,
      :username,
      :password,
      :database,
      :port,
      :pool_size,
      :async_insert,
      :read_only_url,
      :insert_protocol,
      :native_port,
      :native_pool_size
    ])
    |> Logflare.Utils.default_field_value(:async_insert, false)
  end

  @doc false
  @impl Logflare.Backends.Adaptor
  def validate_config(%Changeset{} = changeset) do
    import Ecto.Changeset

    {min_pool, max_pool} = NativeIngester.Pool.pool_size_range()

    changeset
    |> validate_required([:url, :database, :port])
    |> Changeset.validate_format(:url, ~r/https?\:\/\/.+/)
    |> validate_read_only_url()
    |> validate_user_pass()
    |> validate_inclusion(:insert_protocol, ["http", "native"])
    |> validate_number(:pool_size,
      greater_than_or_equal_to: 1,
      less_than_or_equal_to: max_pool
    )
    |> validate_number(:native_pool_size,
      greater_than_or_equal_to: min_pool,
      less_than_or_equal_to: max_pool
    )
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
  Produces a type-specific ingest table name for ClickHouse.

  - `:log`    -> `otel_logs_<token>`
  - `:metric` -> `otel_metrics_<token>`
  - `:trace`  -> `otel_traces_<token>`
  """
  @spec clickhouse_ingest_table_name(Backend.t(), TypeDetection.event_type()) :: String.t()
  def clickhouse_ingest_table_name(%Backend{} = backend, :log),
    do: build_otel_table_name(backend, "otel_logs")

  def clickhouse_ingest_table_name(%Backend{} = backend, :metric),
    do: build_otel_table_name(backend, "otel_metrics")

  def clickhouse_ingest_table_name(%Backend{} = backend, :trace),
    do: build_otel_table_name(backend, "otel_traces")

  @spec build_otel_table_name(Backend.t(), String.t()) :: String.t()
  defp build_otel_table_name(%Backend{token: token}, prefix) do
    token_str = String.replace(token, "-", "_")
    table_name = "#{prefix}_#{token_str}"

    if String.length(table_name) >= 200 do
      raise "The dynamically generated ClickHouse resource name starting with `#{prefix}_` " <>
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

      case Ch.query(pool_via, statement, params, Keyword.put(opts, :decode, false)) do
        {:ok, %Ch.Result{} = result} ->
          {:ok, decode_ch_result(result)}

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
  Inserts a list of `LogEvent` structs into a type-specific ingest table.
  """
  @spec insert_log_events(Backend.t(), [LogEvent.t()], TypeDetection.event_type()) ::
          :ok | {:error, String.t()}
  def insert_log_events(%Backend{}, [], _event_type), do: :ok

  def insert_log_events(
        %Backend{config: %{insert_protocol: "native"}} = backend,
        [%LogEvent{} | _] = events,
        event_type
      )
      when is_event_type(event_type) do
    with :ok <- NativePoolSup.ensure_started(backend) do
      do_insert_log_events(backend, events, event_type, :native)
    end
  end

  def insert_log_events(%Backend{} = backend, [%LogEvent{} | _] = events, event_type)
      when is_event_type(event_type) do
    do_insert_log_events(backend, events, event_type, :http)
  end

  @spec do_insert_log_events(
          Backend.t(),
          [LogEvent.t()],
          TypeDetection.event_type(),
          :http | :native
        ) :: :ok | {:error, String.t()}
  defp do_insert_log_events(backend, events, event_type, protocol) do
    Logger.metadata(backend_id: backend.id)

    table_name = clickhouse_ingest_table_name(backend, event_type)

    result =
      if protocol == :native do
        NativeIngester.insert(backend, table_name, events, event_type)
      else
        Ingester.insert(backend, table_name, events, event_type)
      end

    case result do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error("ClickHouse #{protocol} insert error.",
          error_string: inspect(reason)
        )

        {:error, reason}
    end
  end

  @doc """
  Provisions all type-specific ingest tables for the backend, if they do not already exist.

  Creates one table per log type: `_logs`, `_metrics`, and `_traces`.
  """
  @spec provision_ingest_tables(Backend.t()) :: :ok | {:error, Exception.t()}
  def provision_ingest_tables(%Backend{} = backend) do
    Enum.reduce_while([:log, :metric, :trace], :ok, fn event_type, :ok ->
      table_name = clickhouse_ingest_table_name(backend, event_type)
      statement = QueryTemplates.create_table_statement(table_name, event_type, [])

      case execute_ch_query(backend, statement) do
        {:ok, _} -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
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
        state.pipeline_count + 1

      # Scale up if any queue exceeds threshold
      any_above_threshold? and len > 0 ->
        state.pipeline_count + 1

      # Faster decrease when queues are low
      Enum.all?(lens_no_startup_values, &(&1 < div(@scaling_threshold, 10))) and
        len < @scaling_threshold and state.pipeline_count > 1 and
          (sec_since_last_decr > 30 or state.last_count_decrease == nil) ->
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

  @spec validate_read_only_url(Changeset.t()) :: Changeset.t()
  defp validate_read_only_url(changeset) do
    case Changeset.get_field(changeset, :read_only_url) do
      nil -> changeset
      _url -> Changeset.validate_format(changeset, :read_only_url, ~r/https?\:\/\/.+/)
    end
  end

  @spec decode_ch_result(Ch.Result.t()) :: [map()]
  defp decode_ch_result(%Ch.Result{} = result) do
    format = get_response_header(result.headers, "x-clickhouse-format")

    case format do
      "RowBinaryWithNamesAndTypes" ->
        data = IO.iodata_to_binary(result.data)

        {_names, types, _rest} = parse_row_binary_header(data)
        [names | rows] = Ch.RowBinary.decode_names_and_rows(data)

        uuid_indices = uuid_column_indices(types)
        rows = convert_uuid_values(rows, uuid_indices)

        Enum.map(rows, fn row ->
          names |> Enum.zip(row) |> Map.new()
        end)

      _ ->
        []
    end
  end

  @spec get_response_header([{String.t(), String.t()}], String.t()) :: String.t() | nil
  defp get_response_header(headers, name) when is_list(headers) do
    Enum.find_value(headers, fn {k, v} -> if k == name, do: v end)
  end

  @spec parse_row_binary_header(binary()) :: {[String.t()], [String.t()], binary()}
  defp parse_row_binary_header(data) do
    {num_cols, rest} = decode_varuint(data)
    {names, rest} = decode_n_strings(rest, num_cols, [])
    {types, rest} = decode_n_strings(rest, num_cols, [])
    {names, types, rest}
  end

  defp decode_n_strings(data, 0, acc), do: {Enum.reverse(acc), data}

  defp decode_n_strings(data, n, acc) do
    {string, rest} = decode_lp_string(data)
    decode_n_strings(rest, n - 1, [string | acc])
  end

  defp decode_varuint(<<0::1, byte::7, rest::bytes>>), do: {byte, rest}

  defp decode_varuint(<<1::1, byte::7, rest::bytes>>) do
    {value, rest} = decode_varuint(rest)
    {byte + Bitwise.bsl(value, 7), rest}
  end

  defp decode_lp_string(data) do
    {len, rest} = decode_varuint(data)
    <<string::binary-size(len), rest::bytes>> = rest
    {string, rest}
  end

  @spec uuid_column_indices([String.t()]) :: MapSet.t(non_neg_integer())
  defp uuid_column_indices(type_strings) do
    type_strings
    |> Enum.with_index()
    |> Enum.filter(fn {type, _idx} -> String.contains?(type, "UUID") end)
    |> Enum.map(fn {_type, idx} -> idx end)
    |> MapSet.new()
  end

  defp convert_uuid_values(rows, uuid_indices) when map_size(uuid_indices) == 0, do: rows

  defp convert_uuid_values(rows, uuid_indices),
    do: Enum.map(rows, &convert_row_uuids(&1, uuid_indices))

  defp convert_row_uuids(row, uuid_indices) do
    row
    |> Enum.with_index()
    |> Enum.map(fn {value, idx} ->
      if MapSet.member?(uuid_indices, idx), do: cast_uuid_value(value), else: value
    end)
  end

  defp cast_uuid_value(nil), do: nil
  defp cast_uuid_value(values) when is_list(values), do: Enum.map(values, &cast_uuid_value/1)
  defp cast_uuid_value(<<_::128>> = bin), do: Ecto.UUID.cast!(bin)
  defp cast_uuid_value(other), do: other

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
