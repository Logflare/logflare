defmodule Logflare.Logs.SearchOperations do
  @moduledoc false

  import Ecto.Query
  import Logflare.Ecto.BQQueryAPI
  import Logflare.Logs.SearchQueries

  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.DateTimeUtils
  alias Logflare.Google.BigQuery.GCPConfig
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Logs.SearchOperation, as: SO
  alias Logflare.Logs.SearchOperations.Helpers, as: SearchOperationHelpers
  alias Logflare.Logs.SearchUtils
  alias Logflare.Lql
  alias Logflare.Lql.BackendTransformer.BigQuery, as: BigQueryTransformer
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
    bq_project_id = so.source.user.bigquery_project_id || GCPConfig.default_project_id()
    %{bigquery_dataset_id: dataset_id} = GenUtils.get_bq_user_info(so.source.token)

    with {:ok, response} <-
           BigQueryAdaptor.execute_query(
             {bq_project_id, dataset_id, so.source.user.id},
             so.query,
             query_type: :search
           ) do
      so
      |> SearchUtils.put_result(:query_result, response)
      |> SearchUtils.put_result(:rows, response.rows)
      |> put_sql_string_and_params(response)
    else
      {:error, err} ->
        SearchUtils.put_result(so, :error, err)
    end
  end

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
      from(so.source.bq_table_id)
      |> select(%{})
      |> order_by([t], desc: t.timestamp)
      |> limit(@default_limit)

    %{so | query: query}
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
        Map.put(agg, "datetime", Timex.from_unix(agg["timestamp"], :microsecond))
      end)

    %{so | rows: rows}
  end

  def apply_timestamp_filter_rules(%SO{type: :events} = so) do
    %SO{tailing?: t?, tailing_initial?: ti?, query: query} = so
    chart_period = hd(so.chart_rules).period
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
    query = from(so.source.bq_table_id)
    ts_filters = so.lql_ts_filters

    period =
      so.chart_rules
      |> hd()
      |> Map.get(:period)
      |> to_bq_interval_token()

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
          |> BigQueryTransformer.where_timestamp_ago(
            utc_now,
            tick_count,
            period
          )
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
          SearchOperationHelpers.get_min_max_filter_timestamps(
            ts_filters,
            hd(so.chart_rules).period
          )

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

        query
        |> Lql.apply_filter_rules(ts_filters)
      end

    %{so | query: q}
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

    q = Lql.apply_select_rules(q, select_rules)

    %{so | query: q}
  end

  def apply_filters(%SO{type: :events, query: q} = so) do
    q = Lql.apply_filter_rules(q, so.lql_meta_and_msg_filters)

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

  def apply_numeric_aggs(
        %SO{query: query, chart_rules: chart_rules, lql_meta_and_msg_filters: filter_rules} = so
      ) do
    chart_period = hd(so.chart_rules).period
    chart_path = hd(chart_rules).path

    non_chart_filters =
      Enum.reject(filter_rules, fn %FilterRule{path: path} -> path == chart_path end)

    query =
      query
      |> Lql.apply_filter_rules(non_chart_filters)
      |> order_by([t, ...], desc: 1)

    query = select_timestamp(query, chart_period)

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
