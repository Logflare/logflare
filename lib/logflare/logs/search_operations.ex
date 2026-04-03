defmodule Logflare.Logs.SearchOperations do
  @moduledoc false

  import Ecto.Query
  import Logflare.Ecto.BQQueryAPI
  import Logflare.Logs.SearchQueries

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.DateTimeUtils
  alias Logflare.Google.BigQuery.GCPConfig
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Logs.SearchOperation, as: SO
  alias Logflare.Logs.SearchOperations.Helpers, as: SearchOperationHelpers
  alias Logflare.Logs.SearchUtils
  alias Logflare.Lql
  alias Logflare.Lql.BackendTransformer.BigQuery, as: BigQueryTransformer
  alias Logflare.Lql.BackendTransformer.Postgres, as: PostgresTransformer
  alias Logflare.Lql.Rules
  alias Logflare.Lql.Rules.ChartRule
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.SourceSchemas
  alias Logflare.Sources
  alias Logflare.Utils.Chart, as: ChartUtils
  alias Logflare.Utils.List, as: ListUtils

  @type chart_period :: :day | :hour | :minute | :second
  @type dt_or_ndt :: DateTime.t() | NaiveDateTime.t()

  @default_select_rules "s:timestamp s:id s:event_message"
  @log_level_select_rule "s:metadata.level@level"

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
  def do_query(%SO{} = so) do
    with {:ok, response} <- execute_backend_query(so) do
      response = normalize_query_result(so, response)

      so
      |> SearchUtils.put_result(:query_result, response)
      |> SearchUtils.put_result(:rows, response.rows)
      |> put_sql_string(response)
    else
      {:error, err} ->
        SearchUtils.put_result(so, :error, err)
    end
  end

  @spec normalize_query_result(SO.t(), QueryResult.t()) :: QueryResult.t()
  defp normalize_query_result(%SO{backend_type: :postgres, type: type}, %QueryResult{} = response) do
    normalize_postgres_response(response, type)
  end

  defp normalize_query_result(%SO{}, %QueryResult{} = response), do: response

  @spec execute_backend_query(SO.t()) :: {:ok, map()} | {:error, term()}
  defp execute_backend_query(%SO{backend_type: :postgres} = so) do
    backend = postgres_backend(so)

    PostgresAdaptor.execute_query(backend, so.query, query_type: :search)
  rescue
    error -> {:error, error}
  end

  defp execute_backend_query(%SO{} = so) do
    bq_project_id = so.source.user.bigquery_project_id || GCPConfig.default_project_id()
    %{bigquery_dataset_id: dataset_id} = GenUtils.get_bq_user_info(so.source.token)

    BigQueryAdaptor.execute_query(
      {bq_project_id, dataset_id, so.source.user.id},
      so.query,
      query_type: :search
    )
  end

  @spec put_sql_string(SO.t(), QueryResult.t()) :: SO.t()
  defp put_sql_string(%{sql_string: sql_string} = so, _response) when is_binary(sql_string),
    do: so

  defp put_sql_string(%SO{backend_type: :bigquery} = so, %QueryResult{
         query_string: query_string,
         bq_params: bq_params
       }) do
    %{
      so
      | sql_string: if(is_binary(query_string), do: query_string, else: nil),
        sql_params: if(is_list(bq_params), do: bq_params, else: [])
    }
  end

  defp put_sql_string(%SO{backend_type: :postgres} = so, _response) do
    case PostgresAdaptor.ecto_to_sql(so.query, []) do
      {:ok, {query_string, params}} -> %{so | sql_string: query_string, sql_params: params}
      {:error, _reason} -> so
    end
  end

  @spec apply_query_defaults(SO.t()) :: SO.t()
  def apply_query_defaults(%SO{} = so) do
    query =
      from(table_name(so))
      |> select(%{})
      |> order_by([t], desc: t.timestamp)
      |> limit(@default_limit)

    %{so | query: query}
  end

  @spec apply_halt_conditions(SO.t()) :: SO.t()
  def apply_halt_conditions(%SO{} = so) do
    chart_period = chart_period(so)

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
        chart_period(so)
      )

    if message do
      SearchUtils.put_status(so, :warning, message)
    else
      so
    end
  end

  def put_chart_data_shape_id(
        %SO{
          source: source,
          chart_rules: [%ChartRule{path: "timestamp", aggregate: :count, value_type: :datetime}]
        } = so
      ) do
    flat_type_map = SourceSchemas.source_schema_flatmap_or_default(source)

    cond do
      Map.has_key?(flat_type_map, "metadata.status_code") ->
        %{so | chart_data_shape_id: :netlify_status_codes}

      Map.has_key?(flat_type_map, "metadata.response.status_code") ->
        %{so | chart_data_shape_id: :cloudflare_status_codes}

      Map.has_key?(flat_type_map, "metadata.proxy.statusCode") ->
        %{so | chart_data_shape_id: :vercel_status_codes}

      Map.has_key?(flat_type_map, "metadata.level") ->
        %{so | chart_data_shape_id: :elixir_logger_levels}

      true ->
        so
    end
  end

  def put_chart_data_shape_id(%SO{} = so), do: so

  def put_stats(%SO{stats: stats} = so) do
    stats =
      stats
      |> Map.merge(%{
        total_rows: so.query_result.total_rows,
        total_bytes_processed:
          if(so.query_result.total_bytes_processed == :not_supported,
            do: 0,
            else: so.query_result.total_bytes_processed
          )
      })
      |> Map.put(
        :total_duration,
        System.monotonic_time(:millisecond) - stats.start_monotonic_time
      )

    %{so | stats: stats}
  end

  @spec process_query_result(SO.t()) :: SO.t()
  def process_query_result(%SO{query_result: %QueryResult{rows: rows}, type: :aggregates} = so) do
    rows =
      Enum.map(rows, fn agg ->
        timestamp = normalize_aggregate_timestamp(agg["timestamp"])

        agg
        |> Map.put("timestamp", timestamp)
        |> Map.put("datetime", Timex.from_unix(timestamp, :microsecond))
      end)

    %{so | rows: rows}
  end

  def apply_timestamp_filter_rules(%SO{backend_type: :postgres, type: :events} = so) do
    %{so | query: apply_postgres_event_timestamp_filter_rules(so)}
  end

  def apply_timestamp_filter_rules(%SO{type: :events} = so) do
    %SO{tailing?: t?, tailing_initial?: ti?, query: query} = so
    chart_period = chart_period(so)
    utc_today = Date.utc_today()
    ts_filters = so.lql_ts_filters

    q =
      cond do
        t? and !ti? ->
          case so.partition_by do
            :pseudo ->
              metrics = Sources.get_source_metrics_for_ingest(so.source_token)

              {value, unit} = to_value_unit(metrics.avg)

              query
              |> BigQueryTransformer.where_timestamp_ago(
                utc_today,
                value,
                unit
              )
              |> where([t, ...], in_streaming_buffer())

            :timestamp ->
              query
              |> BigQueryTransformer.where_timestamp_ago(
                DateTime.utc_now(),
                @tailing_timestamp_filter_minutes,
                "MINUTE"
              )
          end

        (t? and ti?) || Enum.empty?(ts_filters) ->
          case so.partition_by do
            :timestamp ->
              query
              |> BigQueryTransformer.where_timestamp_ago(utc_today, 2, "DAY")

            :pseudo ->
              query
              |> BigQueryTransformer.where_timestamp_ago(utc_today, 2, "DAY")
              |> where(
                partition_date() >= bq_date_sub(^utc_today, "2", "DAY") or in_streaming_buffer()
              )
          end

        not Enum.empty?(ts_filters) ->
          %{min: min, max: max} =
            SearchOperationHelpers.get_min_max_filter_timestamps(ts_filters, chart_period)

          case so.partition_by do
            :timestamp ->
              query
              |> where(
                [t],
                fragment("EXTRACT(DATE FROM ?)", t.timestamp) >= ^Timex.to_date(min) and
                  fragment("EXTRACT(DATE FROM ?)", t.timestamp) <= ^Timex.to_date(max)
              )
              |> Lql.apply_filter_rules(ts_filters)

            :pseudo ->
              query
              |> where(
                partition_date() >= ^Timex.to_date(min) and
                  partition_date() <= ^Timex.to_date(max)
              )
              |> or_where(in_streaming_buffer())
              |> Lql.apply_filter_rules(ts_filters)
          end
      end

    %{so | query: q}
  end

  @spec apply_timestamp_filter_rules(SO.t()) :: SO.t()
  def apply_timestamp_filter_rules(%SO{tailing?: t?, type: :aggregates} = so) do
    query = from(table_name(so))
    chart_period = chart_period(so)
    filters = if(t? or Enum.empty?(so.lql_ts_filters), do: [], else: so.lql_ts_filters)

    q =
      case so.backend_type do
        :postgres ->
          %{min: min, max: max} =
            SearchOperationHelpers.get_min_max_filter_timestamps(filters, chart_period)

          query = where(query, [t], t.timestamp >= ^min and t.timestamp <= ^max)

          if Enum.empty?(filters),
            do: query,
            else: Lql.apply_filter_rules(query, filters, dialect: :postgres)

        :bigquery ->
          apply_bq_aggregate_timestamp_filters(query, so, filters, chart_period)
      end

    %{so | query: q}
  end

  defp apply_bq_aggregate_timestamp_filters(query, so, filters, chart_period) do
    period = to_bq_interval_token(chart_period)
    tick_count = SearchOperationHelpers.default_period_tick_count(chart_period)
    utc_today = Date.utc_today()
    utc_now = DateTime.utc_now()

    partition_days =
      case chart_period do
        :day -> 14
        :hour -> 3
        :minute -> 1
        :second -> 1
      end

    if Enum.empty?(filters) do
      query =
        query
        |> BigQueryTransformer.where_timestamp_ago(utc_now, tick_count, period)
        |> limit([t], ^tick_count)

      case so.partition_by do
        :pseudo ->
          where(
            query,
            partition_date() >= bq_date_sub(^utc_today, ^partition_days, "day") or
              in_streaming_buffer()
          )

        :timestamp ->
          query
      end
    else
      %{min: min, max: max} =
        SearchOperationHelpers.get_min_max_filter_timestamps(filters, chart_period)

      query =
        case so.partition_by do
          :pseudo ->
            query
            |> where(
              partition_date() >= ^Timex.to_date(min) and
                partition_date() <= ^Timex.to_date(max)
            )
            |> or_where(in_streaming_buffer())

          :timestamp ->
            query
        end

      Lql.apply_filter_rules(query, filters)
    end
  end

  defp to_value_unit(average) when average < 10, do: {2, "DAY"}
  defp to_value_unit(average) when average < 50, do: {1, "DAY"}
  defp to_value_unit(average) when average < 100, do: {6, "HOUR"}
  defp to_value_unit(average) when average < 200, do: {1, "HOUR"}
  defp to_value_unit(_average), do: {1, "MINUTE"}

  @spec apply_select_rules(SO.t()) :: SO.t()
  def apply_select_rules(%SO{type: :events, query: _q, lql_rules: nil} = so) do
    %{so | lql_rules: []}
    |> apply_select_rules()
  end

  def apply_select_rules(%SO{type: :events, query: q} = so) do
    default_rules = system_select_rules(so)

    select_rules =
      so.lql_rules
      |> Lql.Rules.get_select_rules()
      |> Kernel.++(default_rules)
      |> Rules.SelectRule.normalize()

    q = Lql.apply_select_rules(q, select_rules, dialect: so.backend_type)

    %{so | query: q}
  end

  def apply_filters(%SO{type: :events, query: q} = so) do
    q = Lql.apply_filter_rules(q, so.lql_meta_and_msg_filters, dialect: so.backend_type)

    %{so | query: q}
  end

  @doc """
  Returns a list of SelectRules to be applied to all search queries.
  """
  @spec system_select_rules(SO.t()) :: [Lql.Rules.SelectRule.t()]
  def system_select_rules(%SO{source: source}) do
    source_schema = SourceSchemas.Cache.get_source_schema_by(source_id: source.id)

    flatmap =
      Map.get(source_schema || %{}, :schema_flat_map)

    if flatmap == nil do
      {:ok, rules} =
        @default_select_rules
        |> Lql.Parser.parse()

      Rules.get_select_rules(rules)
    else
      recommended_rules =
        Sources.Source.recommended_query_fields(source)
        |> Enum.map(&recommended_field_to_lql_query/1)

      {:ok, rules} =
        [@default_select_rules, @log_level_select_rule, recommended_rules]
        |> List.flatten()
        |> Enum.join(" ")
        |> Lql.Parser.parse()

      rules
      |> Rules.get_select_rules()
      |> Enum.filter(&Map.has_key?(flatmap, &1.path))
    end
  end

  # converts "m.user_id" to "s:m.user_id@user_id"
  defp recommended_field_to_lql_query(field) when is_binary(field) do
    field = Sources.Source.query_field_name(field)
    field_name = field |> String.split(".") |> List.last()

    "s:#{field}@#{field_name}"
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

  def apply_numeric_aggs(%SO{query: query, lql_meta_and_msg_filters: filter_rules} = so) do
    chart_rule = hd(so.chart_rules)

    non_chart_filters = reject_chart_filters(filter_rules, chart_rule)

    case so.backend_type do
      :postgres ->
        query =
          query
          |> Lql.apply_filter_rules(non_chart_filters)
          |> PostgresTransformer.transform_chart_rule(
            chart_rule.aggregate,
            chart_rule.path,
            chart_rule.period,
            "timestamp"
          )

        %{so | query: query}

      :bigquery ->
        apply_bq_numeric_aggs(so, query, chart_rule, non_chart_filters, filter_rules)
    end
  end

  defp apply_bq_numeric_aggs(so, query, chart_rule, non_chart_filters, filter_rules) do
    chart_period = chart_rule.period

    query =
      query
      |> Lql.apply_filter_rules(non_chart_filters)
      |> order_by([t, ...], desc: 1)

    query = select_timestamp(query, chart_period)
    query = apply_bq_chart_select(query, chart_rule, so.chart_data_shape_id, filter_rules)
    query = group_by(query, 1)

    %{so | query: query}
  end

  defp apply_bq_chart_select(
         query,
         %ChartRule{path: "timestamp", aggregate: :count, value_type: :datetime},
         chart_data_shape_id,
         _filter_rules
       ) do
    case chart_data_shape_id do
      :elixir_logger_levels -> select_count_log_level(query)
      :cloudflare_status_codes -> select_count_cloudflare_http_status_code(query)
      :vercel_status_codes -> select_count_vercel_http_status_code(query)
      :netlify_status_codes -> select_count_netlify_http_status_code(query)
      nil -> select_merge_agg_value(query, :count, :timestamp)
    end
  end

  defp apply_bq_chart_select(
         query,
         %ChartRule{path: p, aggregate: agg},
         _chart_data_shape_id,
         filter_rules
       ) do
    last_chart_field =
      p
      |> String.split(".")
      |> List.last()
      |> String.to_existing_atom()

    q =
      if String.contains?(p, ".") do
        query
        |> Lql.handle_nested_field_access(p)
        |> select_merge_agg_value(agg, last_chart_field, :joined_table)
      else
        query
        |> select_merge_agg_value(agg, last_chart_field, :base_table)
      end

    Enum.reduce(filter_rules, q, fn
      %FilterRule{path: ^p, operator: operator, value: value, modifiers: modifiers}, acc ->
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

  @spec chart_period(SO.t()) :: chart_period()
  defp chart_period(%SO{chart_rules: [%{period: period} | _]}), do: period

  @spec table_name(SO.t()) :: String.t()
  defp table_name(%SO{backend_type: :postgres, source: source}),
    do: PostgresAdaptor.table_name(source)

  defp table_name(%SO{source: source}), do: source.bq_table_id

  defp postgres_backend(%SO{source: %{user: user}}) when not is_nil(user) do
    Backends.get_default_backend(user)
  end

  defp postgres_backend(%SO{}), do: nil

  @spec apply_postgres_event_timestamp_filter_rules(SO.t()) :: Ecto.Query.t()
  defp apply_postgres_event_timestamp_filter_rules(%SO{} = so) do
    %SO{tailing?: t?, tailing_initial?: ti?, query: query} = so
    ts_filters = so.lql_ts_filters

    cond do
      t? and !ti? ->
        where(
          query,
          [t],
          t.timestamp >=
            ^Timex.shift(DateTime.utc_now(), minutes: -@tailing_timestamp_filter_minutes)
        )

      (t? and ti?) || Enum.empty?(ts_filters) ->
        where(query, [t], t.timestamp >= ^Timex.shift(DateTime.utc_now(), days: -2))

      true ->
        Lql.apply_filter_rules(query, ts_filters, dialect: :postgres)
    end
  end

  @spec normalize_postgres_response(QueryResult.t(), :events | :aggregates) :: QueryResult.t()
  defp normalize_postgres_response(%QueryResult{} = response, :events), do: response

  defp normalize_postgres_response(%QueryResult{} = response, :aggregates) do
    rows =
      Enum.map(response.rows, fn row ->
        case Map.pop(row, "count") do
          {nil, _row} -> row
          {count, row} -> Map.put(row, "value", count)
        end
      end)

    %QueryResult{response | rows: rows}
  end

  defp normalize_aggregate_timestamp([timestamp]), do: normalize_aggregate_timestamp(timestamp)
  defp normalize_aggregate_timestamp(timestamp), do: timestamp

  @spec reject_chart_filters([FilterRule.t()], ChartRule.t()) :: [FilterRule.t()]
  defp reject_chart_filters(filter_rules, %ChartRule{path: chart_path}) do
    Enum.reject(filter_rules, fn %FilterRule{path: path} -> path == chart_path end)
  end

  def add_missing_agg_timestamps(%SO{} = so) do
    chart_period = chart_period(so)

    %{min: min, max: max} =
      SearchOperationHelpers.get_min_max_filter_timestamps(so.lql_ts_filters, chart_period)

    if min == max do
      so
    else
      rows = intersperse_missing_range_timestamps(so.rows, min, max, chart_period)

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
          "datetime" => dt,
          "value" => 0
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
