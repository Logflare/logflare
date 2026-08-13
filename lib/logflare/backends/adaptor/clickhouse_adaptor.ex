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

  alias __MODULE__.CircuitBreaker
  alias __MODULE__.ConnectionManager
  alias __MODULE__.EndpointUtils
  alias __MODULE__.Ingester
  alias __MODULE__.Pipeline
  alias __MODULE__.Provisioner
  alias __MODULE__.QueryConnectionSup
  alias __MODULE__.QueryTemplates
  alias Ecto.Changeset
  alias Logflare.Backends
  alias Logflare.Ecto.ClickHouse, as: EctoClickHouse
  alias Logflare.Backends.Backend
  alias Logflare.Backends.DynamicPipeline
  alias Logflare.Backends.Ecto.SqlUtils
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.Backends.QueryError
  alias Logflare.LogEvent
  alias Logflare.LogEvent.TypeDetection
  alias Logflare.Sources.Source
  alias Logflare.Sql.DialectTransformer.ClickHouse, as: ClickHouseSqlTransformer

  @min_pipelines 1
  @resolve_interval 10_000
  @scaling_threshold 15_000
  @async_insert_busy_timeout_max_ms 3_000
  @max_read_pool_size 4096
  @ch_slow_pool_checkout_ms 1_000
  @us_per_hour 3_600 * 1_000_000
  @default_max_event_age_hours 24

  defdelegate connection_pool_via(arg), to: ConnectionManager
  defdelegate connection_pool_via(arg, label), to: ConnectionManager

  @doc false
  def child_spec(arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]}
    }
  end

  @impl Logflare.Backends.Adaptor
  def consolidated_ingest?, do: true

  @impl Logflare.Backends.Adaptor
  def on_backend_config_changed(%Backend{id: backend_id}) do
    QueryConnectionSup.refresh_backend(backend_id)
  end

  @impl Logflare.Backends.Adaptor
  def on_backend_deleted(%Backend{id: backend_id}) do
    QueryConnectionSup.terminate_backend(backend_id)
  end

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

  def execute_query(%Backend{} = backend, {query_string, params}, opts)
      when is_non_empty_binary(query_string) and is_list(params) and is_list(opts) do
    case execute_ch_query(backend, query_string, params, opts) do
      {:ok, {rows, bytes}} -> {:ok, QueryResult.new(rows, %{total_bytes_processed: bytes})}
      error -> error
    end
  end

  def execute_query(%Backend{} = backend, {query_string, declared_params, input_params}, opts)
      when is_non_empty_binary(query_string) and is_list(declared_params) and is_map(input_params) and
             is_list(opts) do
    execute_query_with_params(backend, query_string, declared_params, input_params, opts)
  end

  def execute_query(
        %Backend{} = backend,
        {query_string, declared_params, input_params, endpoint_query},
        opts
      )
      when is_non_empty_binary(query_string) and is_list(declared_params) and is_map(input_params) and
             is_list(opts) do
    with {:ok, {limited_query, max_rows}} <- limit_endpoint_query(query_string, endpoint_query) do
      execute_query_with_params(
        backend,
        limited_query,
        declared_params,
        input_params,
        opts,
        max_rows
      )
    end
  end

  def execute_query(%Backend{} = backend, %Ecto.Query{} = query, opts) when is_list(opts) do
    with {:ok, {ch_sql, ch_params}} <- ecto_to_sql(query, opts) do
      execute_query(backend, {ch_sql, ch_params}, opts)
    end
  end

  @impl Logflare.Backends.Adaptor
  def supports_default_ingest?, do: true

  @doc """
  Default max event age in hours.
  """
  @spec default_max_event_age_hours() :: pos_integer()
  def default_max_event_age_hours, do: @default_max_event_age_hours

  @doc """
  Drops events older than the configured max age before they are
  queued for the consolidated pipeline.

  Late-arriving events land in older partitions, which increases the parts written
  per insert.

  To disable the check, set the `max_event_age_
  Set the backend's `max_event_age_hours`
  config to `0` to disable the check.
  """
  @impl Logflare.Backends.Adaptor
  @spec pre_ingest(Source.t(), Backend.t(), [LogEvent.t()]) :: [LogEvent.t()]
  def pre_ingest(_source, _backend, []), do: []

  def pre_ingest(%Source{} = source, %Backend{} = backend, log_events) do
    case max_event_age_hours(backend) do
      0 -> log_events
      hours -> reject_stale_events(source, backend, log_events, hours)
    end
  end

  @spec max_event_age_hours(Backend.t()) :: non_neg_integer()
  defp max_event_age_hours(%Backend{config: %{max_event_age_hours: hours}})
       when is_non_negative_integer(hours),
       do: hours

  defp max_event_age_hours(_backend), do: @default_max_event_age_hours

  @spec reject_stale_events(Source.t(), Backend.t(), [LogEvent.t()], pos_integer()) :: [
          LogEvent.t()
        ]
  defp reject_stale_events(source, backend, log_events, max_age_hours) do
    min_allowed = System.system_time(:microsecond) - max_age_hours * @us_per_hour

    case Enum.count(log_events, &stale?(&1, min_allowed)) do
      0 ->
        log_events

      dropped ->
        log_stale_drop(source, backend, dropped, length(log_events), max_age_hours)
        Enum.reject(log_events, &stale?(&1, min_allowed))
    end
  end

  @spec stale?(LogEvent.t(), integer()) :: boolean()
  defp stale?(%LogEvent{body: %{"timestamp" => timestamp}}, min_allowed)
       when is_integer(timestamp),
       do: timestamp < min_allowed

  defp stale?(_log_event, _min_allowed), do: false

  @spec log_stale_drop(Source.t(), Backend.t(), pos_integer(), pos_integer(), pos_integer()) ::
          :ok
  defp log_stale_drop(source, backend, dropped, total, max_age_hours) do
    Logger.warning(
      "Dropping #{dropped} of #{total} ClickHouse event(s): timestamps older than #{max_age_hours}h",
      source_id: source.token,
      backend_id: backend.id,
      old_events_dropped: dropped,
      total_event_count: total
    )

    :telemetry.execute(
      [:logflare, :logs, :ingest_logs, :drop_stale],
      %{count: dropped},
      %{
        source_id: source.id,
        source_token: source.token,
        backend_id: backend.id,
        backend_type: :clickhouse
      }
    )
  end

  @doc false
  @impl Logflare.Backends.Adaptor
  def cast_config(%{} = params, existing_config \\ %{}) do
    {existing_config,
     %{
       url: :string,
       username: :string,
       password: :string,
       database: :string,
       port: :integer,
       pool_size: :integer,
       # read_only_url is depreciated and will be removed in the release after PR#3693 lands
       read_only_url: :string,
       read_only_urls: {:map, :string},
       default_read_cluster: :string,
       use_async_inserts_for_small_batches: :boolean,
       async_insert_cluster_url: :string,
       async_insert_max_rows: :integer,
       max_event_age_hours: :integer
     }}
    |> Changeset.cast(params, [
      :url,
      :username,
      :password,
      :database,
      :port,
      :pool_size,
      :read_only_url,
      :read_only_urls,
      :default_read_cluster,
      :use_async_inserts_for_small_batches,
      :async_insert_cluster_url,
      :async_insert_max_rows,
      :max_event_age_hours
    ])
    |> Logflare.Utils.default_field_value(:use_async_inserts_for_small_batches, false)
    |> Logflare.Utils.default_field_value(:async_insert_max_rows, 1_000)
    |> Logflare.Utils.default_field_value(
      :max_event_age_hours,
      @default_max_event_age_hours
    )
    |> strip_url_credentials()
  end

  @spec strip_url_credentials(Changeset.t()) :: Changeset.t()
  defp strip_url_credentials(%Changeset{types: types} = changeset) do
    types
    |> Map.keys()
    |> Enum.filter(&url_field?/1)
    |> Enum.reduce(changeset, fn field, acc ->
      Changeset.update_change(acc, field, &strip_credentials/1)
    end)
  end

  @spec url_field?(atom()) :: boolean()
  defp url_field?(field), do: field |> Atom.to_string() |> String.contains?("url")

  @spec strip_credentials(term()) :: term()
  defp strip_credentials(urls) when is_map(urls) do
    Map.new(urls, fn {label, url} -> {label, EndpointUtils.strip_credentials(url)} end)
  end

  defp strip_credentials(url), do: EndpointUtils.strip_credentials(url)

  @doc false
  @impl Logflare.Backends.Adaptor
  def validate_config(%Changeset{} = changeset) do
    import Ecto.Changeset

    changeset
    |> validate_required([:url, :database, :port])
    |> Changeset.validate_format(:url, ~r/https?\:\/\/.+/)
    |> Changeset.validate_format(:async_insert_cluster_url, ~r/https?\:\/\/.+/)
    |> validate_number(:async_insert_max_rows, greater_than: 0)
    |> validate_number(:max_event_age_hours, greater_than_or_equal_to: 0)
    |> validate_read_only_url()
    |> validate_read_only_urls()
    |> validate_default_read_cluster()
    |> validate_user_pass()
    |> validate_number(:pool_size,
      greater_than_or_equal_to: 1,
      less_than_or_equal_to: @max_read_pool_size
    )
  end

  @doc """
  GRANT checks to verify the configured user has the required ClickHouse permissions.

  Always checks the ingest cluster (primary `url`) for full write permissions.

  Then checks each configured read cluster for `SELECT` permission.

  When async inserts are enabled and a parsable `async_insert_cluster_url` is configured,
  additionally checks that endpoint for connectivity and write permissions.
  """
  @impl Logflare.Backends.Adaptor
  @spec test_connection(Backend.t()) ::
          :ok
          | {:error, :permissions_missing}
          | {:error, :read_permissions_missing}
          | {:error, :async_permissions_missing}
          | {:error, :grant_check_unknown_failure}
  def test_connection(%Backend{config: config} = backend) do
    with :ok <- check_ingest_grants(backend, config),
         :ok <- check_read_grants(backend, config),
         :ok <- maybe_check_async_grants(backend, config) do
      :ok
    end
  end

  @spec check_ingest_grants(Backend.t(), map()) ::
          :ok | {:error, :permissions_missing} | {:error, :grant_check_unknown_failure}
  defp check_ingest_grants(%Backend{} = backend, config) do
    sql_statement = QueryTemplates.grant_check_statement()

    case execute_direct_query(config.url, config, sql_statement) do
      {:ok, [%{"result" => 1}]} ->
        :ok

      {:ok, [%{"result" => 0}]} ->
        Logger.warning(
          "ClickHouse ingest cluster GRANT check failed for #{config.url}. Required: `CREATE TABLE`, `ALTER TABLE`, `INSERT`, `SELECT`, `DROP TABLE`, `CREATE VIEW`, `DROP VIEW`",
          backend_id: backend.id,
          ingest_url: config.url
        )

        {:error, :permissions_missing}

      {:error, _} = error_result ->
        Logger.warning(
          "ClickHouse ingest cluster connection/GRANT check failed for #{config.url}. Unexpected error #{inspect(error_result)}",
          backend_id: backend.id,
          ingest_url: config.url
        )

        {:error, :grant_check_unknown_failure}
    end
  end

  @spec check_read_grants(Backend.t(), map()) ::
          :ok | {:error, :read_permissions_missing} | {:error, term()}
  defp check_read_grants(%Backend{} = backend, config) do
    config
    |> read_grant_targets()
    |> Enum.reduce_while(:ok, fn {label, url}, :ok ->
      case check_read_grant(backend, config, label, url) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  @spec read_grant_targets(map()) :: [{String.t() | nil, String.t()}]
  defp read_grant_targets(config) do
    case config |> Map.get(:read_only_urls, %{}) |> Map.to_list() do
      [] ->
        if is_non_empty_binary(Map.get(config, :read_only_url)),
          do: [{nil, config.read_only_url}],
          else: []

      labeled ->
        labeled
    end
  end

  @spec check_read_grant(Backend.t(), map(), String.t() | nil, String.t()) ::
          :ok | {:error, :read_permissions_missing} | {:error, term()}
  defp check_read_grant(%Backend{} = backend, config, label, url) do
    sql_statement = QueryTemplates.read_grant_check_statement()
    target = describe_read_target(label, url)

    case execute_direct_query(url, config, sql_statement) do
      {:ok, [%{"result" => 1}]} ->
        :ok

      {:ok, [%{"result" => 0}]} ->
        Logger.warning(
          "ClickHouse read cluster GRANT check failed for #{target}. Required: `SELECT`",
          backend_id: backend.id,
          read_cluster: label,
          read_cluster_url: url
        )

        {:error, :read_permissions_missing}

      {:error, _} = error_result ->
        Logger.warning(
          "ClickHouse read cluster connection/GRANT check failed for #{target}. Unexpected error #{inspect(error_result)}",
          backend_id: backend.id,
          read_cluster: label,
          read_cluster_url: url
        )

        {:error, :grant_check_unknown_failure}
    end
  end

  @spec describe_read_target(String.t() | nil, String.t()) :: String.t()
  defp describe_read_target(label, url) when is_non_empty_binary(label), do: "#{label} (#{url})"
  defp describe_read_target(_label, url), do: url

  @doc """
  Resolves a read cluster label
  """
  @spec resolve_read_cluster_label(Backend.t() | map(), String.t() | nil) :: String.t() | nil
  def resolve_read_cluster_label(%Backend{config: config}, read_cluster),
    do: resolve_read_cluster_label(config, read_cluster)

  def resolve_read_cluster_label(config, read_cluster) when is_map(config) do
    urls = Map.get(config, :read_only_urls) || %{}

    if is_non_empty_binary(read_cluster) and Map.has_key?(urls, read_cluster) do
      read_cluster
    else
      default_read_cluster_label(config, urls)
    end
  end

  @spec default_read_cluster_label(Backend.t()) :: String.t() | nil
  defp default_read_cluster_label(%Backend{config: config}) do
    urls = Map.get(config, :read_only_urls) || %{}
    default_read_cluster_label(config, urls)
  end

  @spec default_read_cluster_label(map(), map()) :: String.t() | nil
  defp default_read_cluster_label(config, urls) do
    default = Map.get(config, :default_read_cluster)
    if is_non_empty_binary(default) and Map.has_key?(urls, default), do: default, else: nil
  end

  @spec maybe_check_async_grants(Backend.t(), map()) ::
          :ok | {:error, :async_permissions_missing} | {:error, :grant_check_unknown_failure}
  defp maybe_check_async_grants(%Backend{} = backend, config) do
    case async_grant_check_url(config) do
      nil -> :ok
      async_url -> check_async_grants(backend, config, async_url)
    end
  end

  # The dedicated async endpoint is only checked when async routing is enabled and a
  # set, parsable `async_insert_cluster_url` is configured.
  @spec async_grant_check_url(map()) :: String.t() | nil
  defp async_grant_check_url(%{
         use_async_inserts_for_small_batches: true,
         async_insert_cluster_url: url
       })
       when is_non_empty_binary(url) do
    case EndpointUtils.host(url) do
      host when is_non_empty_binary(host) -> url
      _ -> nil
    end
  end

  defp async_grant_check_url(_config), do: nil

  @spec check_async_grants(Backend.t(), map(), String.t()) ::
          :ok | {:error, :async_permissions_missing} | {:error, :grant_check_unknown_failure}
  defp check_async_grants(%Backend{} = backend, config, async_url) do
    sql_statement = QueryTemplates.async_insert_grant_check_statement()

    case execute_direct_query(async_url, config, sql_statement) do
      {:ok, [%{"result" => 1}]} ->
        :ok

      {:ok, [%{"result" => 0}]} ->
        Logger.warning(
          "ClickHouse async insert cluster GRANT check failed. Required: `INSERT`, `SELECT`",
          backend_id: backend.id
        )

        {:error, :async_permissions_missing}

      {:error, _} = error_result ->
        Logger.warning(
          "ClickHouse async insert cluster GRANT check failed. Unexpected error #{inspect(error_result)}",
          backend_id: backend.id
        )

        {:error, :grant_check_unknown_failure}
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
        ) :: {:ok, {[map()], non_neg_integer() | :not_supported}} | {:error, QueryError.t()}
  def execute_ch_query(backend, statement, params \\ [], opts \\ [])

  def execute_ch_query(%Backend{} = backend, statement, params, opts)
      when is_list_or_map(params) and is_list(opts) do
    requested = Keyword.get(opts, :read_cluster)
    label = resolve_read_cluster_label(backend, requested)

    warn_on_unconfigured_read_cluster(backend, requested, label)

    case do_ch_query_on_label(backend, statement, params, label) do
      {:error, %QueryError{kind: :connection_error}} = error ->
        maybe_retry_on_default_cluster(backend, statement, params, label, error)

      result ->
        result
    end
  end

  @spec do_ch_query_on_label(Backend.t(), iodata(), term(), String.t() | nil) ::
          {:ok, {[map()], non_neg_integer() | :not_supported}} | {:error, term()}
  defp do_ch_query_on_label(%Backend{} = backend, statement, params, label) do
    with :ok <- ensure_query_connection_manager_started(backend, label) do
      pool_via = connection_pool_via(backend, label)

      timeout = if Application.get_env(:logflare, :env) == :test, do: 1_000, else: 60_000

      backend_id = backend.id
      log_fun = fn entry -> log_slow_checkout(entry, backend_id) end

      ch_opts = [decode: false, timeout: timeout, log: log_fun]

      case Ch.query(pool_via, statement, params, ch_opts) do
        {:ok, %Ch.Result{} = result} ->
          rows = decode_ch_result(result)
          bytes = parse_summary_read_bytes(result.headers)
          {:ok, {rows, bytes}}

        {:error, error} ->
          {:error,
           error
           |> to_query_error()
           |> QueryError.log(
             user_id: backend.user_id,
             backend_id: backend.id,
             backend_token: backend.token,
             read_cluster: label,
             host: ConnectionManager.read_host(backend, label)
           )}
      end
    end
  end

  @spec warn_on_unconfigured_read_cluster(Backend.t(), String.t() | nil, String.t() | nil) :: :ok
  defp warn_on_unconfigured_read_cluster(%Backend{} = backend, requested, label)
       when is_non_empty_binary(requested) and requested != label do
    Logger.warning(
      "ClickHouse read cluster not configured, falling back to resolved read cluster",
      user_id: backend.user_id,
      backend_id: backend.id,
      requested_read_cluster: requested,
      resolved_read_cluster: label
    )

    :ok
  end

  defp warn_on_unconfigured_read_cluster(_backend, _requested, _label), do: :ok

  @spec maybe_retry_on_default_cluster(
          Backend.t(),
          iodata(),
          term(),
          String.t() | nil,
          {:error, term()}
        ) :: {:ok, {[map()], non_neg_integer() | :not_supported}} | {:error, term()}
  defp maybe_retry_on_default_cluster(%Backend{} = backend, statement, params, label, error) do
    default = default_read_cluster_label(backend)

    if is_non_empty_binary(default) and default != label do
      Logger.warning(
        "ClickHouse read cluster unhealthy, falling back to default read cluster",
        user_id: backend.user_id,
        backend_id: backend.id,
        read_cluster: label,
        default_read_cluster: default
      )

      do_ch_query_on_label(backend, statement, params, default)
    else
      error
    end
  end

  @spec log_slow_checkout(DBConnection.LogEntry.t(), pos_integer()) :: :ok
  defp log_slow_checkout(%DBConnection.LogEntry{pool_time: pool_time}, backend_id)
       when is_integer(pool_time) do
    pool_ms = System.convert_time_unit(pool_time, :native, :millisecond)

    if pool_ms >= slow_pool_checkout_ms() do
      Logger.warning(
        "ClickHouse slow connection checkout: waited #{pool_ms}ms for a pool connection",
        backend_id: backend_id
      )
    end

    :ok
  end

  defp log_slow_checkout(_entry, _backend_id), do: :ok

  @spec slow_pool_checkout_ms() :: non_neg_integer()
  defp slow_pool_checkout_ms do
    Application.get_env(:logflare, __MODULE__)[:slow_pool_checkout_ms] ||
      @ch_slow_pool_checkout_ms
  end

  @spec to_query_error(term()) :: QueryError.t()
  defp to_query_error(%Ch.Error{} = error) do
    error
    |> ch_query_error_kind()
    |> query_error(error)
  end

  defp to_query_error(%DBConnection.ConnectionError{} = error) do
    query_error(:connection_error, error)
  end

  defp to_query_error(error) do
    query_error(:backend_error, error)
  end

  @spec ch_query_error_kind(term()) :: QueryError.kind()
  defp ch_query_error_kind(%Ch.Error{code: code}) when code in [47, 62], do: :invalid_query

  defp ch_query_error_kind(%Ch.Error{message: message}) when is_binary(message) do
    if message =~ "UNKNOWN_IDENTIFIER" or message =~ "SYNTAX_ERROR" do
      :invalid_query
    else
      :backend_error
    end
  end

  defp ch_query_error_kind(%Ch.Error{}), do: :backend_error

  @spec query_error(QueryError.kind(), term()) :: QueryError.t()
  defp query_error(kind, raw_error) do
    %QueryError{
      kind: kind,
      raw_error: raw_error,
      backend: __MODULE__
    }
  end

  @spec execute_direct_query(url :: String.t(), config :: map(), statement :: String.t()) ::
          {:ok, list()} | {:error, term()}
  defp execute_direct_query(url, config, statement) do
    {scheme, hostname, port} = EndpointUtils.origin(url, Map.get(config, :port))
    timeout = if Application.get_env(:logflare, :env) == :test, do: 1_000, else: 30_000

    ch_opts = [
      scheme: scheme,
      hostname: hostname,
      port: port,
      database: config.database,
      username: config.username,
      password: config.password,
      pool_size: 1,
      settings: [],
      timeout: timeout
    ]

    case Ch.start_link(ch_opts) do
      {:ok, pid} ->
        try do
          case Ch.query(pid, statement, [], decode: false, timeout: timeout) do
            {:ok, %Ch.Result{} = result} ->
              {:ok, decode_ch_result(result)}

            {:error, error} ->
              {:error, to_query_error(error)}
          end
        after
          GenServer.stop(pid)
        end

      {:error, error} ->
        {:error, to_query_error(error)}
    end
  end

  @doc """
  Inserts a list of `LogEvent` structs into a type-specific ingest table.

  When `opts` includes `async: true`, the insert is routed through ClickHouse
  async inserts so the server coalesces sparse, late-arriving batches into
  fewer, fatter parts.
  """
  @spec insert_log_events(Backend.t(), [LogEvent.t()], TypeDetection.event_type(), keyword()) ::
          :ok | {:error, String.t()}
  def insert_log_events(backend, events, event_type, opts \\ [])

  def insert_log_events(%Backend{}, [], _event_type, _opts), do: :ok

  def insert_log_events(%Backend{} = backend, [%LogEvent{} | _] = events, event_type, opts)
      when is_event_type(event_type) do
    Logger.metadata(backend_id: backend.id)
    table_name = clickhouse_ingest_table_name(backend, event_type)
    async? = Keyword.get(opts, :async, false)
    insert_opts = [{:async, async?} | build_insert_opts(opts)]

    case Ingester.insert(backend, table_name, events, event_type, insert_opts) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("ClickHouse http insert error.",
          host: insert_host(backend.config, async?),
          error_string: inspect(reason)
        )

        {:error, reason}
    end
  end

  @doc """
  Inserts a pre-gzipped RowBinary payload into the appropriate type-specific ingest table.

  Bypasses encoding and compression. Intended for streaming-zlib pipelines.
  """
  @spec insert_log_events_compressed(
          Backend.t(),
          TypeDetection.event_type(),
          compressed :: binary(),
          opts :: keyword()
        ) :: :ok | {:error, term()}
  def insert_log_events_compressed(%Backend{} = backend, event_type, compressed, opts \\ [])
      when is_event_type(event_type) and is_binary(compressed) do
    Logger.metadata(backend_id: backend.id)
    table_name = clickhouse_ingest_table_name(backend, event_type)
    async? = Keyword.get(opts, :async, false)
    insert_opts = [{:async, async?} | build_insert_opts(opts)]

    case Ingester.insert_compressed(backend, table_name, event_type, compressed, insert_opts) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("ClickHouse http insert error.",
          host: insert_host(backend.config, async?),
          error_string: inspect(reason)
        )

        {:error, reason}
    end
  end

  @spec build_insert_opts(keyword()) :: keyword()
  defp build_insert_opts(opts) do
    if Keyword.get(opts, :async, false), do: async_insert_opts(), else: []
  end

  # The endpoint host an HTTP insert actually targets, for failure logging: async inserts
  # hit the dedicated `async_insert_cluster_url` when configured (falling back to the
  # primary URL), mirroring the routing in `Ingester`; everything else hits the primary URL.
  @spec insert_host(term(), boolean()) :: String.t() | nil
  defp insert_host(%{async_insert_cluster_url: async_url} = config, true)
       when is_non_empty_binary(async_url) do
    EndpointUtils.host(async_url) || EndpointUtils.host(Map.get(config, :url))
  end

  defp insert_host(config, _async?) when is_map(config) do
    EndpointUtils.host(Map.get(config, :url))
  end

  defp insert_host(_config, _async?), do: nil

  @spec async_insert_opts() :: keyword()
  defp async_insert_opts do
    [
      async_insert: 1,
      wait_for_async_insert: 1,
      async_insert_busy_timeout_max_ms: @async_insert_busy_timeout_max_ms
    ]
  end

  @doc """
  Provisions all type-specific ingest tables for the backend, if they do not already exist.

  Creates one table per log type: `_logs`, `_metrics`, and `_traces`.
  """
  @spec provision_ingest_tables(Backend.t()) :: :ok | {:error, QueryError.t()}
  def provision_ingest_tables(%Backend{config: config} = backend) do
    cloud? = EndpointUtils.clickhouse_cloud_url?(config[:url])

    Enum.reduce_while([:log, :metric, :trace], :ok, fn event_type, :ok ->
      table_name = clickhouse_ingest_table_name(backend, event_type)
      ddl_opts = build_ddl_opts(event_type, cloud?)
      statement = QueryTemplates.create_table_statement(table_name, event_type, ddl_opts)

      case execute_direct_query(config.url, config, statement) do
        {:ok, _} -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  @spec build_ddl_opts(TypeDetection.event_type(), boolean()) :: Keyword.t()
  defp build_ddl_opts(:log, cloud?), do: [clickhouse_cloud: cloud?]

  defp build_ddl_opts(_event_type, cloud?) do
    [clickhouse_cloud: cloud? and QueryTemplates.apply_cloud_settings_to_all_tables?()]
  end

  @doc false
  @impl Supervisor
  def init(%Backend{} = backend) do
    # create the startup queue and its generation, before any producer/traffic exists
    # for this queues_key — avoids racing concurrent first-time inserts against each

    # other to lazily create the generation (see IngestEventQueue.current_generation_tid/1)

    # IngestEventQueue.upsert_tid({:consolidated, backend.id, nil})
    IngestEventQueue.current_generation_tid({:consolidated, backend.id})

    children =
      if(Application.get_env(:logflare, :env) != :test,
        do: [Provisioner.child_spec(backend)],
        else: []
      ) ++
        [
          CircuitBreaker.child_spec(backend),
          {
            DynamicPipeline,
            name: Backends.via_backend(backend, Pipeline),
            pipeline: Pipeline,
            pipeline_args: [backend: backend],
            min_pipelines: @min_pipelines,
            max_pipelines: 1,
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
  #
  # Exposed (not private) so it can be unit tested directly, same convention as
  # Backends.handle_resolve_count/3 (BigQuery's counterpart).
  @doc false
  @spec resolve_pipeline_count(map(), [{term(), non_neg_integer()}]) :: non_neg_integer()
  def resolve_pipeline_count(state, lens) do
    startup_size = startup_queue_size(lens)

    lens_no_startup =
      Enum.filter(lens, fn
        {{:consolidated, _bid, nil}, _val} -> false
        _ -> true
      end)

    lens_no_startup_values = Enum.map(lens_no_startup, fn {_, v} -> v end)
    len = Enum.map(lens, fn {_, v} -> v end) |> Enum.sum()

    last_decr = state.last_count_decrease || NaiveDateTime.utc_now()
    sec_since_last_decr = NaiveDateTime.diff(NaiveDateTime.utc_now(), last_decr)

    # Gated on every queue being above threshold, not the average: an average can
    # still be dragged over threshold by a single large outlier while every other
    # queue sits idle (e.g. [30_000, 0] and [60_000, 0, 0, 0] both average to
    # exactly @scaling_threshold with empty queues in the mix). Weighted routing
    # (see IngestEventQueue.weight_by_load/2) already fills the least-loaded queue
    # preferentially, so if even that one is over threshold the fleet genuinely
    # needs the extra pipeline.
    fleet_above_threshold? =
      lens_no_startup_values != [] and
        Enum.all?(lens_no_startup_values, &(&1 >= @scaling_threshold))

    cond do
      # Scale up if startup queue has events (pipeline not yet ready)
      startup_size > 0 ->
        state.pipeline_count + 1

      # Scale up only if the fleet is loaded on average, not just one outlier
      fleet_above_threshold? and len > 0 ->
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

  @spec startup_queue_size([{term(), non_neg_integer()}]) :: non_neg_integer()
  defp startup_queue_size(lens) do
    Enum.find_value(lens, 0, fn
      {{:consolidated, _bid, nil}, value} -> value
      _ -> false
    end)
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

  @spec validate_read_only_urls(Changeset.t()) :: Changeset.t()
  defp validate_read_only_urls(changeset) do
    changeset
    |> Changeset.get_field(:read_only_urls)
    |> case do
      urls when is_map(urls) -> Enum.reduce(urls, changeset, &validate_read_only_url_entry/2)
      _ -> changeset
    end
  end

  @spec validate_read_only_url_entry({String.t(), term()}, Changeset.t()) :: Changeset.t()
  defp validate_read_only_url_entry({label, url}, changeset) do
    if is_non_empty_binary(url) and Regex.match?(~r/https?\:\/\/.+/, url) do
      changeset
    else
      Changeset.add_error(changeset, :read_only_urls, "invalid URL for read cluster \"#{label}\"")
    end
  end

  @spec validate_default_read_cluster(Changeset.t()) :: Changeset.t()
  defp validate_default_read_cluster(changeset) do
    urls = Changeset.get_field(changeset, :read_only_urls) || %{}
    default = Changeset.get_field(changeset, :default_read_cluster)

    validate_default_read_cluster(changeset, urls, default)
  end

  @spec validate_default_read_cluster(Changeset.t(), map(), term()) :: Changeset.t()
  defp validate_default_read_cluster(changeset, urls, _default) when map_size(urls) == 0,
    do: changeset

  defp validate_default_read_cluster(changeset, _urls, default)
       when not is_non_empty_binary(default),
       do: changeset

  defp validate_default_read_cluster(changeset, urls, default) do
    if Map.has_key?(urls, default) do
      changeset
    else
      Changeset.add_error(
        changeset,
        :default_read_cluster,
        "must match one of the defined read cluster labels"
      )
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

  @spec parse_summary_read_bytes([{String.t(), String.t()}]) :: non_neg_integer() | :not_supported
  defp parse_summary_read_bytes(headers) do
    with raw when is_binary(raw) <- get_response_header(headers, "x-clickhouse-summary"),
         {:ok, %{"read_bytes" => bytes}} <- Jason.decode(raw),
         {int, _} <- Integer.parse(to_string(bytes)) do
      int
    else
      _ -> :not_supported
    end
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
    <<string::binary-size(^len), rest::bytes>> = rest
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

  defp convert_uuid_values(rows, uuid_indices) do
    if MapSet.size(uuid_indices) > 0 do
      Enum.map(rows, &convert_row_uuids(&1, uuid_indices))
    else
      rows
    end
  end

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
          input_params :: map(),
          opts :: Keyword.t()
        ) ::
          {:ok, QueryResult.t()} | {:error, any()}
  defp execute_query_with_params(
         %Backend{} = backend,
         query_string,
         declared_params,
         input_params,
         opts
       ) do
    execute_query_with_params(backend, query_string, declared_params, input_params, opts, nil)
  end

  @spec execute_query_with_params(
          Backend.t(),
          query_string :: String.t(),
          declared_params :: [String.t()],
          input_params :: map(),
          opts :: Keyword.t(),
          max_rows :: pos_integer() | nil
        ) ::
          {:ok, QueryResult.t()} | {:error, any()}
  defp execute_query_with_params(
         %Backend{} = backend,
         query_string,
         declared_params,
         input_params,
         opts,
         max_rows
       ) do
    converted_query = convert_query_params(query_string, declared_params)
    ch_params = Map.take(input_params, declared_params)

    case execute_ch_query(backend, converted_query, ch_params, opts) do
      {:ok, {rows, bytes}} ->
        rows = if is_pos_integer(max_rows), do: Enum.take(rows, max_rows), else: rows
        {:ok, QueryResult.new(rows, %{total_bytes_processed: bytes})}

      error ->
        error
    end
  end

  @spec limit_endpoint_query(String.t(), term()) ::
          {:ok, {String.t(), pos_integer() | nil}} | {:error, String.t()}
  defp limit_endpoint_query(query_string, %{max_limit: max_limit})
       when is_pos_integer(max_limit) do
    with {:ok, limited_query} <- ClickHouseSqlTransformer.apply_limit(query_string, max_limit) do
      {:ok, {limited_query, max_limit}}
    end
  end

  defp limit_endpoint_query(query_string, _endpoint_query), do: {:ok, {query_string, nil}}

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

  @spec ensure_query_connection_manager_started(Backend.t(), String.t() | nil) ::
          :ok | {:error, term()}
  defp ensure_query_connection_manager_started(%Backend{id: backend_id} = backend, label) do
    via = Backends.via_backend(backend, ConnectionManager, label)

    via
    |> GenServer.whereis()
    |> maybe_start_query_connection_manager(backend_id, label)
    |> case do
      :ok -> ensure_pool_and_notify(backend, label)
      error -> error
    end
  end

  @spec maybe_start_query_connection_manager(pid() | nil, pos_integer(), String.t() | nil) ::
          :ok | {:error, term()}
  defp maybe_start_query_connection_manager(nil, backend_id, label)
       when is_pos_integer(backend_id) do
    backend = Backends.Cache.get_backend(backend_id)

    with child_spec <- ConnectionManager.child_spec(backend, label),
         {:ok, _pid} <- QueryConnectionSup.start_connection_manager(child_spec) do
      Logger.info(
        "Started query ConnectionManager for ClickHouse backend",
        backend_id: backend.id,
        read_cluster: label
      )

      :ok
    else
      {:error, {:already_started, _pid}} ->
        :ok

      {:error, reason} = error ->
        Logger.warning(
          "Failed to start query ConnectionManager for backend",
          backend_id: backend_id,
          read_cluster: label,
          reason: reason
        )

        error
    end
  end

  defp maybe_start_query_connection_manager(_pid, _backend_id, _label), do: :ok

  @spec ensure_pool_and_notify(Backend.t(), String.t() | nil) :: :ok
  defp ensure_pool_and_notify(%Backend{} = backend, label) do
    ConnectionManager.ensure_pool_started(backend, label)
    ConnectionManager.notify_activity(backend, label)
    :ok
  end
end
