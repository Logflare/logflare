defmodule Logflare.Logs.SearchOperations do
  @moduledoc false
  alias Logflare.BqRepo
  alias Logflare.DateTimeUtils
  alias Logflare.Google.BigQuery.{GenUtils}
  alias Logflare.Google.BigQuery.GCPConfig
  alias Logflare.Logs.Search.Utils
  alias Logflare.Lql
  alias Logflare.EctoQueryBQ
  alias Logflare.SourceSchemas
  alias Logflare.Sources

  import Ecto.Query

  alias Logflare.Ecto.BQQueryAPI

  import Logflare.Logs.SearchOperations.Helpers
  import Logflare.Logs.Search.Utils
  import Logflare.Logs.SearchQueries
  import BQQueryAPI

  alias Logflare.Logs.SearchOperation, as: SO

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

  @spec do_query(SO.t()) :: SO.t()
  def do_query(%SO{} = so) do
    bq_project_id = so.source.user.bigquery_project_id || GCPConfig.default_project_id()
    bytes_limit = so.source.user.bigquery_processed_bytes_limit
    %{bigquery_dataset_id: dataset_id} = GenUtils.get_bq_user_info(so.source.token)

    query_total_bytes_halt =
      "Query halted: total bytes processed for this query is expected to be larger than #{div(bytes_limit, 1_000_000_000)} GB"

    with {:ok, response} <-
           BqRepo.query(so.source.user, bq_project_id, so.query,
             dataset_id: dataset_id,
             dryRun: true
           ),
         is_within_limit? = response.total_bytes_processed <= bytes_limit,
         {:total_bytes_processed, true} <- {:total_bytes_processed, is_within_limit?},
         {:ok, response} <-
           BqRepo.query(so.source.user, bq_project_id, so.query, dataset_id: dataset_id) do
      so
      |> Utils.put_result(:query_result, response)
      |> Utils.put_result(:rows, response.rows)
    else
      {:total_bytes_processed, false} ->
        Utils.halt(so, query_total_bytes_halt)

      {:error, err} ->
        Utils.put_result(so, :error, err)
    end
  end

  @spec apply_query_defaults(SO.t()) :: SO.t()
  def apply_query_defaults(%SO{} = so) do
    query =
      from(so.source.bq_table_id)
      |> select([t], [t.timestamp, t.id, t.event_message])
      |> order_by([t], desc: t.timestamp)
      |> limit(@default_limit)

    %{so | query: query}
  end

  def unnest_log_level(so) do
    query =
      so.query
      |> join(:inner, [t], m in fragment("UNNEST(?)", t.metadata), on: true)
      |> select_merge([..., m], %{
        level: fragment("JSON_EXTRACT_SCALAR(TO_JSON_STRING(?), '$.level') as level", m)
      })

    %{so | query: query}
  end

  @spec apply_halt_conditions(SO.t()) :: SO.t()
  def apply_halt_conditions(%SO{} = so) do
    chart_period = hd(so.chart_rules).period

    %{min: min_ts, max: max_ts} = get_min_max_filter_timestamps(so.lql_ts_filters, chart_period)

    cond do
      so.tailing? and not Enum.empty?(so.lql_ts_filters) ->
        Utils.halt(so, @timestamp_filter_with_tailing)

      Logflare.Utils.List.at_least?(so.chart_rules, 2) ->
        Utils.halt(so, "Only one chart rule can be used in a query")

      match?([_], so.chart_rules) and
        hd(so.chart_rules).value_type not in ~w[integer float]a and
          hd(so.chart_rules).path != "timestamp" ->
        chart_rule = hd(so.chart_rules)

        msg =
          "Can't aggregate on a non-numeric field type '#{chart_rule.value_type}' for path #{chart_rule.path}. Check the source schema for the field used with chart operator."

        Utils.halt(so, msg)

      Timex.diff(max_ts, min_ts, chart_period) == 0 and chart_period != :second ->
        msg =
          "Selected chart period #{chart_period} is longer than the timestamp filter interval. Please select a shorter chart period."

        Utils.halt(so, msg)

      get_number_of_chart_ticks(min_ts, max_ts, chart_period) > @default_max_n_chart_ticks ->
        msg =
          "The interval length between min and max timestamp is larger than #{@default_max_n_chart_ticks} periods, please use a longer chart aggregation period."

        Utils.halt(so, msg)

      true ->
        so
    end
  end

  @spec apply_warning_conditions(SO.t()) :: SO.t()
  def apply_warning_conditions(%SO{} = so) do
    %{message: message} =
      get_min_max_filter_timestamps(so.lql_ts_filters, hd(so.chart_rules).period)

    if message do
      put_status(so, :warning, message)
    else
      so
    end
  end

  def put_chart_data_shape_id(%SO{} = so) do
    flat_type_map =
      SourceSchemas.get_source_schema_by(source_id: so.source.id)
      |> Map.get(:schema_flat_map)

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

              {value, unit} =
                cond do
                  metrics.avg < 10 ->
                    {2, "DAY"}

                  metrics.avg < 50 ->
                    {1, "DAY"}

                  metrics.avg < 100 ->
                    {6, "HOUR"}

                  metrics.avg < 200 ->
                    {1, "HOUR"}

                  true ->
                    {1, "MINUTE"}
                end

              query
              |> Lql.EctoHelpers.where_timestamp_ago(utc_today, value, unit)
              |> where([t, ...], in_streaming_buffer())

            :timestamp ->
              query
              |> Lql.EctoHelpers.where_timestamp_ago(
                DateTime.utc_now(),
                @tailing_timestamp_filter_minutes,
                "MINUTE"
              )
          end

        (t? and ti?) || Enum.empty?(ts_filters) ->
          case so.partition_by do
            :timestamp ->
              query
              |> Lql.EctoHelpers.where_timestamp_ago(utc_today, 2, "DAY")

            :pseudo ->
              query
              |> Lql.EctoHelpers.where_timestamp_ago(utc_today, 2, "DAY")
              |> where(
                partition_date() >= bq_date_sub(^utc_today, "2", "DAY") or in_streaming_buffer()
              )
          end

        not Enum.empty?(ts_filters) ->
          %{min: min, max: max} = get_min_max_filter_timestamps(ts_filters, chart_period)

          case so.partition_by do
            :timestamp ->
              query
              |> where(
                [t],
                fragment("EXTRACT(DATE FROM ?)", t.timestamp) >= ^Timex.to_date(min) and
                  fragment("EXTRACT(DATE FROM ?)", t.timestamp) <= ^Timex.to_date(max)
              )
              |> Lql.EctoHelpers.apply_filter_rules_to_query(ts_filters)

            :pseudo ->
              query
              |> where(
                partition_date() >= ^Timex.to_date(min) and
                  partition_date() <= ^Timex.to_date(max)
              )
              |> or_where(in_streaming_buffer())
              |> Lql.EctoHelpers.apply_filter_rules_to_query(ts_filters)
          end
      end

    %{so | query: q}
  end

  @spec apply_timestamp_filter_rules(SO.t()) :: SO.t()
  def apply_timestamp_filter_rules(%SO{tailing?: t?, type: :aggregates} = so) do
    query = from(so.source.bq_table_id)

    ts_filters = so.lql_ts_filters

    period =
      hd(so.chart_rules).period
      |> Logflare.Ecto.BQQueryAPI.to_bq_interval_token()

    tick_count = default_period_tick_count(hd(so.chart_rules).period)

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
          |> Logflare.Lql.EctoHelpers.where_timestamp_ago(utc_now, tick_count, period)
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
          get_min_max_filter_timestamps(ts_filters, hd(so.chart_rules).period)

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
        |> Lql.EctoHelpers.apply_filter_rules_to_query(ts_filters)
      end

    %{so | query: q}
  end

  def apply_filters(%SO{type: :events, query: q} = so) do
    q = Lql.EctoHelpers.apply_filter_rules_to_query(q, so.lql_meta_and_msg_filters)

    %{so | query: q}
  end

  @spec apply_to_sql(SO.t()) :: SO.t()
  def apply_to_sql(%SO{} = so) do
    %{bigquery_dataset_id: bq_dataset_id} = GenUtils.get_bq_user_info(so.source.token)
    {sql, params} = EctoQueryBQ.SQL.to_sql_params(so.query)
    sql = EctoQueryBQ.SQL.substitute_dataset(sql, bq_dataset_id)
    sql_and_params = {sql, params}
    sql_with_params_string = EctoQueryBQ.SQL.sql_params_to_sql(sql_and_params)
    %{so | sql_params: sql_and_params, sql_string: sql_with_params_string}
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

    query =
      query
      |> Lql.EctoHelpers.apply_filter_rules_to_query(so.lql_meta_and_msg_filters)
      |> order_by([t, ...], desc: 1)

    query = select_timestamp(query, chart_period)

    query =
      case chart_rules do
        [%Logflare.Lql.ChartRule{path: "timestamp", aggregate: :count, value_type: :datetime}] ->
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

        [%Logflare.Lql.ChartRule{value_type: _, path: p, aggregate: agg}] ->
          last_chart_field =
            p
            |> String.split(".")
            |> List.last()
            |> String.to_existing_atom()

          q =
            query
            |> Lql.EctoHelpers.unnest_and_join_nested_columns(:inner, p)
            |> select_merge_agg_value(agg, last_chart_field)

          Enum.reduce(filter_rules, q, fn
            %Logflare.Lql.FilterRule{
              path: ^p,
              operator: operator,
              value: value,
              modifiers: modifiers
            },
            acc ->
              last_filter_field =
                p
                |> String.split(".")
                |> List.last()
                |> String.to_existing_atom()

              where(
                acc,
                ^Lql.EctoHelpers.dynamic_where_filter_rule(
                  last_filter_field,
                  operator,
                  value,
                  modifiers
                )
              )

            %Logflare.Lql.FilterRule{}, acc ->
              acc
          end)
      end

    query = group_by(query, 1)

    %{so | query: query}
  end

  def add_missing_agg_timestamps(%SO{} = so) do
    %{min: min, max: max} =
      get_min_max_filter_timestamps(so.lql_ts_filters, hd(so.chart_rules).period)

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
