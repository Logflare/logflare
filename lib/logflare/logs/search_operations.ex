defmodule Logflare.Logs.SearchOperations do
  @moduledoc false

  import Ecto.Query
  import Logflare.Ecto.BQQueryAPI
  import Logflare.Logs.SearchQueries

  require Logger

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Backends.Adaptor.ClickhouseAdaptor
  alias Logflare.DateTimeUtils
  alias Logflare.Google.BigQuery.GCPConfig
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Logs.SearchOperation, as: SO
  alias Logflare.Logs.SearchOperations.Helpers, as: SearchOperationHelpers
  alias Logflare.Logs.SearchUtils
  alias Logflare.Lql
  alias Logflare.Lql.BackendTransformer.BigQuery, as: BigQueryTransformer
  alias Logflare.Lql.BackendTransformer.ClickHouse, as: ClickHouseTransformer
  alias Logflare.Lql.Rules.ChartRule
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.SingleTenant
  alias Logflare.SourceSchemas
  alias Logflare.Sources
  alias Logflare.Utils.Chart, as: ChartUtils
  alias Logflare.Utils.List, as: ListUtils

  @type chart_period :: :day | :hour | :minute | :second
  @type dt_or_ndt :: DateTime.t() | NaiveDateTime.t()

  @default_limit 100
  @default_max_n_chart_ticks 1_000
  @tailing_timestamp_filter_minutes 10
  # Note that this is only a timeout for the request, not the query.
  # If the query takes longer to run than the timeout value, the call returns without any results and with the
  # 'jobComplete' flag set to false.

  # Halt reasons

  @timestamp_filter_with_tailing "Timestamp filters can't be used if live tail search is active"

  @spec max_chart_ticks :: integer()
  def max_chart_ticks, do: @default_max_n_chart_ticks

  @spec do_query(SO.t()) :: SO.t()
  def do_query(%SO{backend: nil} = so) do
    # Single-tenant mode: use system default backend
    with {:ok, response} <- execute_single_tenant_query(so) do
      so
      |> SearchUtils.put_result(:query_result, response)
      |> SearchUtils.put_result(:rows, response.rows)
      |> put_sql_string_and_params(response)
    else
      {:error, err} ->
        SearchUtils.put_result(so, :error, err)
    end
  end

  def do_query(%SO{backend: backend} = so) do
    # Multi-tenant mode: use explicit backend
    adaptor = Adaptor.get_adaptor(backend)

    with {:ok, response} <- execute_backend_query(adaptor, backend, so) do
      so
      |> SearchUtils.put_result(:query_result, response)
      |> SearchUtils.put_result(:rows, response.rows)
      |> put_sql_string_and_params(response)
    else
      {:error, err} ->
        SearchUtils.put_result(so, :error, err)
    end
  end

  defp execute_backend_query(BigQueryAdaptor, _backend, %SO{} = so) do
    bq_project_id = so.source.user.bigquery_project_id || GCPConfig.default_project_id()
    %{bigquery_dataset_id: dataset_id} = GenUtils.get_bq_user_info(so.source.token)

    BigQueryAdaptor.execute_query(
      {bq_project_id, dataset_id, so.source.user.id},
      so.query,
      query_type: :search
    )
  end

  defp execute_backend_query(ClickhouseAdaptor, backend, %SO{} = so) do
    case ClickhouseAdaptor.execute_query(backend, so.query, query_type: :search) do
      {:ok, rows} when is_list(rows) ->
        {:ok,
         %{
           rows: rows,
           total_bytes_processed: 0,
           total_rows: length(rows),
           query_string: "",
           bq_params: []
         }}

      error ->
        error
    end
  end

  # Single-tenant query execution
  defp execute_single_tenant_query(%SO{} = so) do
    case SingleTenant.backend_type() do
      :bigquery ->
        bq_project_id = so.source.user.bigquery_project_id || GCPConfig.default_project_id()
        %{bigquery_dataset_id: dataset_id} = GenUtils.get_bq_user_info(so.source.token)

        BigQueryAdaptor.execute_query(
          {bq_project_id, dataset_id, so.source.user.id},
          so.query,
          query_type: :search
        )

      :clickhouse ->
        execute_clickhouse_single_tenant_query(so)

      :postgres ->
        # PostgreSQL backend uses BigQuery-compatible SQL
        {:error, "PostgreSQL backend search not yet implemented for single-tenant"}
    end
  end

  # Direct ClickHouse execution for single-tenant mode (bypasses connection manager)
  defp execute_clickhouse_single_tenant_query(%SO{} = so) do
    with {:ok, {ch_sql, ch_params}} <-
           ClickhouseAdaptor.ecto_to_sql(so.query, query_type: :search),
         _ <- Logger.info("ClickHouse query: #{ch_sql} | Params: #{inspect(ch_params)}"),
         {:ok, pool_name} <- ensure_clickhouse_pool_started(),
         {:ok, %Ch.Result{} = result} <- Ch.query(pool_name, ch_sql, ch_params) do
      rows = convert_ch_result_to_rows(result)
      Logger.info("ClickHouse result: #{length(rows)} rows | Data: #{inspect(rows)}")

      {:ok,
       %{
         rows: rows,
         total_bytes_processed: 0,
         total_rows: length(rows),
         query_string: ch_sql,
         bq_params: ch_params
       }}
    else
      {:error, %Ch.Error{message: error_msg}} ->
        Logger.error("ClickHouse single-tenant query failed: #{inspect(error_msg)}")
        {:error, "Error executing ClickHouse query: #{error_msg}"}

      {:error, reason} = error ->
        Logger.error("ClickHouse single-tenant query failed: #{inspect(reason)}")
        error
    end
  end

  @single_tenant_pool_name Logflare.ClickHouse.SingleTenantPool

  # Ensures the single-tenant ClickHouse pool is started and returns its name
  defp ensure_clickhouse_pool_started do
    case GenServer.whereis(@single_tenant_pool_name) do
      nil ->
        # Pool not started, start it
        start_clickhouse_pool()

      pid when is_pid(pid) ->
        # Pool already started
        {:ok, @single_tenant_pool_name}
    end
  end

  defp start_clickhouse_pool do
    case SingleTenant.clickhouse_backend_adapter_opts() do
      nil ->
        {:error, "ClickHouse backend not configured"}

      opts ->
        with {:ok, {scheme, hostname}} <- extract_scheme_and_hostname(opts[:url]) do
          ch_opts = [
            name: @single_tenant_pool_name,
            scheme: scheme,
            hostname: hostname,
            port: opts[:port] || extract_port_from_url(opts[:url]),
            database: opts[:database],
            username: opts[:username],
            password: opts[:password],
            pool_size: 5,
            settings: [],
            timeout: :timer.minutes(1)
          ]

          case Ch.start_link(ch_opts) do
            {:ok, _pid} ->
              Logger.info("Started ClickHouse single-tenant connection pool")
              {:ok, @single_tenant_pool_name}

            {:error, {:already_started, _pid}} ->
              {:ok, @single_tenant_pool_name}

            {:error, reason} = error ->
              Logger.error("Failed to start ClickHouse pool: #{inspect(reason)}")
              error
          end
        end
    end
  end

  defp extract_scheme_and_hostname(url) when is_binary(url) do
    case URI.new(url) do
      {:ok, %URI{scheme: scheme, host: hostname}} when scheme in ~w(http https) ->
        {:ok, {scheme, hostname}}

      {:ok, %URI{}} ->
        {:error, "Invalid URL scheme or missing hostname"}

      {:error, _} ->
        {:error, "Failed to parse URL"}
    end
  end

  defp extract_port_from_url(url) when is_binary(url) do
    case URI.parse(url) do
      %URI{port: port} when is_integer(port) -> port
      _ -> 8123
    end
  end

  # Copied from ClickhouseAdaptor - converts Ch.Result to list of maps
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

  defp convert_uuids(data) when is_struct(data), do: data

  defp convert_uuids(data) when is_map(data) do
    Map.new(data, fn {k, v} -> {k, convert_uuids(v)} end)
  end

  defp convert_uuids(data) when is_list(data) do
    Enum.map(data, &convert_uuids/1)
  end

  defp convert_uuids(data) when is_binary(data) and byte_size(data) == 16 do
    Ecto.UUID.cast!(data)
  end

  defp convert_uuids(data), do: data

  # Returns the correct table name based on backend type
  defp get_table_name(%SO{backend: %{type: :bigquery}, source: source}), do: source.bq_table_id

  defp get_table_name(%SO{backend: %{type: :clickhouse}, source: source}) do
    ClickhouseAdaptor.clickhouse_ingest_table_name(source)
  end

  # Single-tenant mode: determine table name from system default backend
  defp get_table_name(%SO{backend: nil, source: source}) do
    case SingleTenant.backend_type() do
      :clickhouse -> ClickhouseAdaptor.clickhouse_ingest_table_name(source)
      :postgres -> source.bq_table_id
      :bigquery -> source.bq_table_id
    end
  end

  defp get_backend_transformer(%SO{backend: %{type: :bigquery}}), do: BigQueryTransformer
  defp get_backend_transformer(%SO{backend: %{type: :clickhouse}}), do: ClickHouseTransformer

  # Single-tenant mode: no explicit backend, use system default
  defp get_backend_transformer(%SO{backend: nil}) do
    case SingleTenant.backend_type() do
      :clickhouse -> ClickHouseTransformer
      :postgres -> BigQueryTransformer
      :bigquery -> BigQueryTransformer
    end
  end

  # Converts transformer module to dialect atom for LQL operations
  defp transformer_to_dialect(BigQueryTransformer), do: :bigquery
  defp transformer_to_dialect(ClickHouseTransformer), do: :clickhouse

  # Backend-specific timestamp truncation for aggregations
  defp select_timestamp_for_backend(query, :second, ClickHouseTransformer) do
    import Ecto.Query

    select(query, [t], %{
      timestamp: fragment("toStartOfInterval(?, INTERVAL 1 second)", t.timestamp)
    })
  end

  defp select_timestamp_for_backend(query, :minute, ClickHouseTransformer) do
    import Ecto.Query

    select(query, [t], %{
      timestamp: fragment("toStartOfInterval(?, INTERVAL 1 minute)", t.timestamp)
    })
  end

  defp select_timestamp_for_backend(query, :hour, ClickHouseTransformer) do
    import Ecto.Query

    select(query, [t], %{
      timestamp: fragment("toStartOfInterval(?, INTERVAL 1 hour)", t.timestamp)
    })
  end

  defp select_timestamp_for_backend(query, :day, ClickHouseTransformer) do
    import Ecto.Query

    select(query, [t], %{timestamp: fragment("toStartOfInterval(?, INTERVAL 1 day)", t.timestamp)})
  end

  defp select_timestamp_for_backend(query, chart_period, _bigquery_transformer) do
    # Use the existing BigQuery-compatible select_timestamp function
    select_timestamp(query, chart_period)
  end

  defp supports_streaming_buffer?(%SO{backend: %{type: :bigquery}}), do: true
  defp supports_streaming_buffer?(%SO{backend: %{type: :clickhouse}}), do: false
  defp supports_streaming_buffer?(_), do: false

  @spec put_sql_string_and_params(SO.t(), %{query_string: String.t(), bq_params: list()}) ::
          SO.t()
  defp put_sql_string_and_params(%{sql_string: sql_string} = so, _response)
       when is_binary(sql_string),
       do: so

  defp put_sql_string_and_params(so, %{query_string: query_string, bq_params: bq_params}) do
    %{so | sql_string: query_string, sql_params: bq_params}
  end

  @spec apply_query_defaults(SO.t()) :: SO.t()
  def apply_query_defaults(%SO{} = so) do
    query =
      from(get_table_name(so))
      |> select([t], [t.timestamp, t.id, t.event_message])
      |> order_by([t], desc: t.timestamp)
      |> limit(@default_limit)

    %{so | query: query}
  end

  def unnest_log_level(%_{source: %{id: source_id}} = so) do
    source_schema = SourceSchemas.Cache.get_source_schema_by(source_id: source_id)
    flatmap = Map.get(source_schema || %{}, :schema_flat_map)

    if is_map_key(flatmap || %{}, "metadata.level") do
      query =
        so.query
        |> join(:inner, [t], m in fragment("UNNEST(?)", t.metadata), on: true)
        |> select_merge([..., m], %{
          level: m.level
        })

      %{so | query: query}
    else
      so
    end
  end

  @spec apply_halt_conditions(SO.t()) :: SO.t()
  def apply_halt_conditions(%SO{} = so) do
    chart_period = hd(so.chart_rules).period

    %{min: min_ts, max: max_ts} =
      SearchOperationHelpers.get_min_max_filter_timestamps(so.lql_ts_filters, chart_period)

    cond do
      so.tailing? and not Enum.empty?(so.lql_ts_filters) ->
        SearchUtils.halt(so, @timestamp_filter_with_tailing)

      ListUtils.at_least?(so.chart_rules, 2) ->
        SearchUtils.halt(so, "Only one chart rule can be used in a query")

      Timex.diff(max_ts, min_ts, chart_period) == 0 and chart_period != :second ->
        msg =
          "Selected chart period #{chart_period} is longer than the timestamp filter interval. Please select a shorter chart period."

        SearchUtils.halt(so, msg)

      ChartUtils.get_number_of_chart_ticks(min_ts, max_ts, chart_period) >
          @default_max_n_chart_ticks ->
        msg =
          "The interval length between min and max timestamp is larger than #{@default_max_n_chart_ticks} periods, please use a longer chart aggregation period."

        SearchUtils.halt(so, msg)

      true ->
        so
    end
  end

  @spec apply_warning_conditions(SO.t()) :: SO.t()
  def apply_warning_conditions(%SO{} = so) do
    %{message: message} =
      SearchOperationHelpers.get_min_max_filter_timestamps(
        so.lql_ts_filters,
        hd(so.chart_rules).period
      )

    if message do
      SearchUtils.put_status(so, :warning, message)
    else
      so
    end
  end

  def put_chart_data_shape_id(%SO{} = so) do
    flat_type_map =
      SourceSchemas.Cache.get_source_schema_by(source_id: so.source.id)
      |> case do
        %_{schema_flat_map: flatmap} -> flatmap || %{}
        _ -> %{}
      end

    chart_data_shape_id =
      cond do
        Map.has_key?(flat_type_map, "metadata.status_code") ->
          :netlify_status_codes

        Map.has_key?(flat_type_map, "metadata.response.status_code") ->
          :cloudflare_status_codes

        Map.has_key?(flat_type_map, "metadata.proxy.statusCode") ->
          :vercel_status_codes

        Map.has_key?(flat_type_map, "metadata.level") ->
          :elixir_logger_levels

        true ->
          nil
      end

    Map.put(so, :chart_data_shape_id, chart_data_shape_id)
  end

  def put_stats(%SO{stats: stats} = so) do
    stats =
      stats
      |> Map.merge(%{
        total_rows: so.query_result.total_rows,
        total_bytes_processed: so.query_result.total_bytes_processed
      })
      |> Map.put(
        :total_duration,
        System.monotonic_time(:millisecond) - stats.start_monotonic_time
      )

    %{so | stats: stats}
  end

  @spec process_query_result(SO.t()) :: SO.t()
  def process_query_result(%SO{query_result: %{rows: rows}, type: :aggregates} = so) do
    rows =
      Enum.map(rows, fn agg ->
        agg =
          Map.update!(agg, "timestamp", fn ts ->
            case ts do
              %NaiveDateTime{} = naive_datetime ->
                naive_datetime
                |> DateTime.from_naive!("Etc/UTC")
                |> DateTime.to_unix(:microsecond)

              int when is_integer(int) ->
                int
            end
          end)

        datetime = Timex.from_unix(agg["timestamp"], :microsecond)

        Map.put(agg, "datetime", datetime)
      end)

    %{so | rows: rows}
  end

  def apply_timestamp_filter_rules(%SO{type: :events} = so) do
    %SO{tailing?: t?, tailing_initial?: ti?, query: query} = so
    chart_period = hd(so.chart_rules).period
    utc_today = Date.utc_today()
    ts_filters = so.lql_ts_filters
    transformer = get_backend_transformer(so)
    supports_streaming? = supports_streaming_buffer?(so)

    q =
      cond do
        t? and !ti? ->
          apply_tailing_timestamp_filter(so, query, transformer, supports_streaming?, utc_today)

        (t? and ti?) || Enum.empty?(ts_filters) ->
          apply_default_timestamp_filter(so, query, transformer, supports_streaming?, utc_today)

        not Enum.empty?(ts_filters) ->
          apply_explicit_timestamp_filter(
            so,
            query,
            transformer,
            supports_streaming?,
            ts_filters,
            chart_period
          )
      end

    %{so | query: q}
  end

  @spec apply_timestamp_filter_rules(SO.t()) :: SO.t()
  def apply_timestamp_filter_rules(%SO{tailing?: t?, type: :aggregates} = so) do
    query = from(get_table_name(so))
    ts_filters = so.lql_ts_filters
    transformer = get_backend_transformer(so)
    supports_streaming? = supports_streaming_buffer?(so)

    period =
      so.chart_rules
      |> hd()
      |> Map.get(:period)
      |> Logflare.Ecto.BQQueryAPI.to_bq_interval_token()

    tick_count =
      so.chart_rules
      |> hd()
      |> Map.get(:period)
      |> SearchOperationHelpers.default_period_tick_count()

    utc_today = Date.utc_today()
    utc_now = DateTime.utc_now()

    partition_days =
      case hd(so.chart_rules).period do
        :day -> 14
        :hour -> 3
        :minute -> 1
        :second -> 1
      end

    q =
      if t? or Enum.empty?(ts_filters) do
        query =
          query
          |> transformer.where_timestamp_ago(
            utc_now,
            tick_count,
            period
          )
          |> limit([t], ^tick_count)

        case so.partition_by do
          :pseudo when supports_streaming? ->
            where(
              query,
              partition_date() >= bq_date_sub(^utc_today, ^partition_days, "day") or
                in_streaming_buffer()
            )

          :pseudo ->
            query

          :timestamp ->
            query
        end
      else
        %{min: min, max: max} =
          SearchOperationHelpers.get_min_max_filter_timestamps(
            ts_filters,
            hd(so.chart_rules).period
          )

        query =
          case so.partition_by do
            :pseudo when supports_streaming? ->
              query
              |> where(
                partition_date() >= ^Timex.to_date(min) and
                  partition_date() <= ^Timex.to_date(max)
              )
              |> or_where(in_streaming_buffer())

            :pseudo ->
              query

            :timestamp ->
              query
          end

        query
        |> Lql.apply_filter_rules(ts_filters, dialect: transformer_to_dialect(transformer))
      end

    %{so | query: q}
  end

  defp apply_tailing_timestamp_filter(
         %SO{partition_by: :pseudo, source_token: source_token},
         query,
         transformer,
         supports_streaming?,
         utc_today
       ) do
    metrics = Sources.get_source_metrics_for_ingest(source_token)
    {value, unit} = to_value_unit(metrics.avg)

    query = transformer.where_timestamp_ago(query, utc_today, value, unit)

    if supports_streaming? do
      where(query, [t, ...], in_streaming_buffer())
    else
      query
    end
  end

  defp apply_tailing_timestamp_filter(
         %SO{partition_by: :timestamp},
         query,
         transformer,
         _supports_streaming?,
         _utc_today
       ) do
    transformer.where_timestamp_ago(
      query,
      DateTime.utc_now(),
      @tailing_timestamp_filter_minutes,
      "MINUTE"
    )
  end

  defp apply_default_timestamp_filter(
         %SO{partition_by: :timestamp},
         query,
         transformer,
         _supports_streaming?,
         utc_today
       ) do
    transformer.where_timestamp_ago(query, utc_today, 2, "DAY")
  end

  defp apply_default_timestamp_filter(
         %SO{partition_by: :pseudo},
         query,
         transformer,
         supports_streaming?,
         utc_today
       ) do
    query = transformer.where_timestamp_ago(query, utc_today, 2, "DAY")

    if supports_streaming? do
      where(
        query,
        partition_date() >= bq_date_sub(^utc_today, "2", "DAY") or in_streaming_buffer()
      )
    else
      query
    end
  end

  defp apply_explicit_timestamp_filter(
         %SO{partition_by: :timestamp},
         query,
         transformer,
         _supports_streaming?,
         ts_filters,
         chart_period
       ) do
    %{min: min, max: max} =
      SearchOperationHelpers.get_min_max_filter_timestamps(ts_filters, chart_period)

    # Backend-specific date extraction
    min_date = Timex.to_date(min)
    max_date = Timex.to_date(max)

    query =
      case transformer do
        ClickHouseTransformer ->
          # ClickHouse: toDate() function
          query
          |> where(
            [t],
            fragment("toDate(?)", t.timestamp) >= ^min_date and
              fragment("toDate(?)", t.timestamp) <= ^max_date
          )

        _ ->
          # BigQuery: EXTRACT(DATE FROM ?)
          query
          |> where(
            [t],
            fragment("EXTRACT(DATE FROM ?)", t.timestamp) >= ^min_date and
              fragment("EXTRACT(DATE FROM ?)", t.timestamp) <= ^max_date
          )
      end

    query
    |> Lql.apply_filter_rules(ts_filters, dialect: transformer_to_dialect(transformer))
  end

  defp apply_explicit_timestamp_filter(
         %SO{partition_by: :pseudo},
         query,
         transformer,
         supports_streaming?,
         ts_filters,
         chart_period
       ) do
    %{min: min, max: max} =
      SearchOperationHelpers.get_min_max_filter_timestamps(ts_filters, chart_period)

    query =
      where(
        query,
        partition_date() >= ^Timex.to_date(min) and
          partition_date() <= ^Timex.to_date(max)
      )

    query =
      if supports_streaming? do
        or_where(query, in_streaming_buffer())
      else
        query
      end

    Lql.apply_filter_rules(query, ts_filters, dialect: transformer_to_dialect(transformer))
  end

  defp to_value_unit(average) when average < 10, do: {2, "DAY"}
  defp to_value_unit(average) when average < 50, do: {1, "DAY"}
  defp to_value_unit(average) when average < 100, do: {6, "HOUR"}
  defp to_value_unit(average) when average < 200, do: {1, "HOUR"}
  defp to_value_unit(_average), do: {1, "MINUTE"}

  def apply_filters(%SO{type: :events, query: q} = so) do
    transformer = get_backend_transformer(so)

    q =
      Lql.apply_filter_rules(q, so.lql_meta_and_msg_filters,
        dialect: transformer_to_dialect(transformer)
      )

    %{so | query: q}
  end

  @spec apply_local_timestamp_correction(SO.t()) :: SO.t()
  def apply_local_timestamp_correction(%SO{} = so) do
    lql_ts_filters =
      so.lql_ts_filters
      |> Enum.map(fn
        %{path: "timestamp", values: values, operator: :range} = pvo ->
          values =
            for value <- values do
              value
              |> Timex.to_datetime(so.search_timezone || "Etc/UTC")
              |> Timex.Timezone.convert("Etc/UTC")
            end

          %{pvo | values: values}

        %{path: "timestamp", value: value} = pvo ->
          value =
            value
            |> Timex.to_datetime(so.search_timezone || "Etc/UTC")
            |> Timex.Timezone.convert("Etc/UTC")

          %{pvo | value: value}
      end)

    %{so | lql_ts_filters: lql_ts_filters}
  end

  def apply_numeric_aggs(
        %SO{query: query, chart_rules: chart_rules, lql_meta_and_msg_filters: filter_rules} = so
      ) do
    chart_period = hd(so.chart_rules).period
    transformer = get_backend_transformer(so)

    query =
      query
      |> Lql.apply_filter_rules(so.lql_meta_and_msg_filters,
        dialect: transformer_to_dialect(transformer)
      )
      |> order_by([t, ...], desc: 1)

    query = select_timestamp_for_backend(query, chart_period, transformer)

    query =
      case chart_rules do
        [%ChartRule{path: "timestamp", aggregate: :count, value_type: :datetime}] ->
          case so.chart_data_shape_id do
            :elixir_logger_levels ->
              select_count_log_level(query)

            :cloudflare_status_codes ->
              select_count_cloudflare_http_status_code(query)

            :vercel_status_codes ->
              select_count_vercel_http_status_code(query)

            :netlify_status_codes ->
              select_count_netlify_http_status_code(query)

            nil ->
              select_merge_agg_value(query, :count, :timestamp)
          end

        [%ChartRule{value_type: _, path: p, aggregate: agg}] ->
          last_chart_field =
            p
            |> String.split(".")
            |> List.last()
            |> String.to_existing_atom()

          # Only create UNNEST joins for nested fields (containing ".")
          # Top-level fields should reference the base table directly
          is_nested_field = String.contains?(p, ".")

          q =
            if is_nested_field do
              query
              |> Lql.handle_nested_field_access(p)
              |> select_merge_agg_value(agg, last_chart_field, :joined_table)
            else
              query
              |> select_merge_agg_value(agg, last_chart_field, :base_table)
            end

          Enum.reduce(filter_rules, q, fn
            %FilterRule{
              path: ^p,
              operator: operator,
              value: value,
              modifiers: modifiers
            },
            acc ->
              where(
                acc,
                ^Lql.transform_filter_rule(%FilterRule{
                  path: p,
                  operator: operator,
                  value: value,
                  modifiers: modifiers
                })
              )

            %FilterRule{}, acc ->
              acc
          end)
      end

    query = group_by(query, 1)

    %{so | query: query}
  end

  def add_missing_agg_timestamps(%SO{} = so) do
    %{min: min, max: max} =
      SearchOperationHelpers.get_min_max_filter_timestamps(
        so.lql_ts_filters,
        hd(so.chart_rules).period
      )

    if min == max do
      so
    else
      rows = intersperse_missing_range_timestamps(so.rows, min, max, hd(so.chart_rules).period)

      %{so | rows: rows}
    end
  end

  @spec intersperse_missing_range_timestamps(list(map), dt_or_ndt, dt_or_ndt, chart_period) ::
          list(map)
  def intersperse_missing_range_timestamps(aggs, min, max, chart_period) do
    use Timex

    step_period = String.to_existing_atom("#{chart_period}s")
    from = DateTimeUtils.truncate(min, chart_period)
    until = DateTimeUtils.truncate(max, chart_period)

    until =
      if from == until and step_period == :seconds do
        Timex.shift(until, seconds: 1)
      else
        until
      end

    empty_aggs =
      Interval.new(
        from: from,
        until: until,
        left_open: false,
        right_open: false,
        step: [{step_period, 1}]
      )
      |> Enum.to_list()
      |> Enum.map(fn dt ->
        ts = dt |> DateTime.from_naive!("Etc/UTC") |> DateTime.to_unix(:microsecond)

        %{
          "timestamp" => ts,
          "datetime" => dt
        }
      end)

    [aggs | empty_aggs]
    |> List.flatten()
    |> Enum.uniq_by(& &1["timestamp"])
    |> Enum.sort_by(& &1["timestamp"], :desc)
  end

  def put_time_stats(%SO{} = so) do
    %{
      so
      | stats: %{
          start_monotonic_time: System.monotonic_time(:millisecond),
          total_duration: nil
        }
    }
  end
end
