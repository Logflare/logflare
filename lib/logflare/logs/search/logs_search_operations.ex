defmodule Logflare.Logs.SearchOperations do
  @moduledoc false
  alias Logflare.Google.BigQuery.{GenUtils, SchemaUtils}
  alias Logflare.{Sources, EctoQueryBQ}
  alias Logflare.Lql.Parser
  alias Logflare.Lql
  alias Logflare.Logs.Search.Utils
  alias Logflare.Lql.{ChartRule, FilterRule}
  alias Logflare.BqRepo
  alias Logflare.Google.BigQuery.GCPConfig

  import Ecto.Query

  alias Logflare.Ecto.BQQueryAPI

  import Logflare.Logs.SearchOperations.Helpers
  import Logflare.Logs.Search.Utils
  import Logflare.Logs.SearchQueries
  import BQQueryAPI.UDF
  import BQQueryAPI

  alias Logflare.Logs.SearchOperation, as: SO

  @type chart_period :: :day | :hour | :minute | :second

  @default_limit 100
  @default_processed_bytes_limit 10_000_000_000
  @default_max_n_chart_ticks 250

  # Note that this is only a timeout for the request, not the query.
  # If the query takes longer to run than the timeout value, the call returns without any results and with the 'jobComplete' flag set to false.

  # Halt reasons

  @timestamp_filter_with_tailing "Timestamp filters can't be used if live tail search is active"
  @query_total_bytes_halt "Query halted: total bytes processed for this query is expected to be larger than #{
                            div(@default_processed_bytes_limit, 1_000_000_000)
                          } GB"

  @spec do_query(SO.t()) :: SO.t()
  def do_query(%SO{} = so) do
    bq_project_id = so.source.user.bigquery_project_id || GCPConfig.default_project_id()
    %{bigquery_dataset_id: dataset_id} = GenUtils.get_bq_user_info(so.source.token)

    with {:ok, response} <-
           BqRepo.query(bq_project_id, so.query, dataset_id: dataset_id, dryRun: true),
         is_within_limit? = response.total_bytes_processed <= @default_processed_bytes_limit,
         {:total_bytes_processed, true} <- {:total_bytes_processed, is_within_limit?},
         {:ok, response} <- BqRepo.query(bq_project_id, so.query, dataset_id: dataset_id) do
      so
      |> Utils.put_result(:query_result, response)
      |> Utils.put_result(:rows, response.rows)
    else
      {:total_bytes_processed, false} ->
        Utils.halt(so, @query_total_bytes_halt)

      {:error, err} ->
        Utils.put_result(so, :error, err)
    end
  end

  @spec apply_query_defaults(SO.t()) :: SO.t()
  def apply_query_defaults(%SO{type: :events} = so) do
    query =
      from(so.source.bq_table_id)
      |> select_default_fields(:events)
      |> order_by(desc: :timestamp)
      |> limit(@default_limit)

    %{so | query: query}
  end

  @spec apply_halt_conditions(SO.t()) :: SO.t()
  def apply_halt_conditions(%SO{} = so) do
    %{min: min_ts, max: max_ts} =
      get_min_max_filter_timestamps(so.lql_ts_filters, so.chart_period)

    cond do
      so.tailing? and not Enum.empty?(so.lql_ts_filters) ->
        Utils.halt(so, @timestamp_filter_with_tailing)

      length(so.chart_rules) > 1 ->
        Utils.halt(so, "Only one chart rule can be used in a query")

      match?([_], so.chart_rules) and
        hd(so.chart_rules).value_type in ~w[integer float]a and
          hd(so.chart_rules).path != "timestamp" ->
        chart_rule = hd(so.chart_rules)

        msg =
          "Error: can't aggregate on a non-numeric field type '#{chart_rule.value_type}' for path #{
            chart_rule.path
          }. Check the source schema for the field used with chart operator."

        Utils.halt(so, msg)

      get_number_of_chart_ticks(min_ts, max_ts, so.chart_period) > @default_max_n_chart_ticks ->
        msg =
          "the interval length between min and max timestamp is larger than #{
            @default_max_n_chart_ticks
          }, please select use another chart aggregation period."

        Utils.halt(so, msg)

      true ->
        so
    end
  end

  @spec apply_warning_conditions(SO.t()) :: SO.t()
  def apply_warning_conditions(%SO{} = so) do
    %{message: message} = get_min_max_filter_timestamps(so.lql_ts_filters, so.chart_period)

    if message do
      put_status(so, :warning, message)
    else
      so
    end
  end

  def put_chart_data_shape_id(%SO{} = so) do
    flat_type_map =
      so.source
      |> Sources.Cache.get_bq_schema()
      |> SchemaUtils.bq_schema_to_flat_typemap()

    chart_data_shape_id =
      cond do
        not Enum.empty?(so.chart_rules) ->
          nil

        Map.has_key?(flat_type_map, "metadata.level") ->
          :elixir_logger_levels

        Map.has_key?(flat_type_map, "metadata.response.status_code") ->
          :cloudflare_status_codes

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
        Map.put(agg, :datetime, Timex.from_unix(agg.timestamp, :microsecond))
      end)

    %{so | rows: rows}
  end

  def apply_timestamp_filter_rules(%SO{type: :events} = so) do
    %SO{tailing?: t?, tailing_initial?: ti?, query: query} = so

    utc_today = Date.utc_today()

    ts_filters = so.lql_ts_filters

    q =
      cond do
        t? and !ti? ->
          query
          |> where([t, ...], t.timestamp >= lf_timestamp_sub(^utc_today, 2, "DAY"))
          |> where([t, ...], in_streaming_buffer())

        (t? and ti?) || Enum.empty?(ts_filters) ->
          query
          |> where([t, ...], t.timestamp >= lf_timestamp_sub(^utc_today, 2, "DAY"))
          |> where(
            partition_date() >= bq_date_sub(^utc_today, "2", "DAY") or in_streaming_buffer()
          )

        not Enum.empty?(ts_filters) ->
          %{min: min, max: max} = get_min_max_filter_timestamps(ts_filters, so.chart_period)

          query
          |> where(
            partition_date() >= ^Timex.to_date(min) and partition_date() <= ^Timex.to_date(max)
          )
          |> or_where(in_streaming_buffer())
          |> Lql.EctoHelpers.apply_filter_rules_to_query(ts_filters)
      end

    %{so | query: q}
  end

  @spec apply_timestamp_filter_rules(SO.t()) :: SO.t()
  def apply_timestamp_filter_rules(%SO{tailing?: t?, type: :aggregates} = so) do
    query = from(so.source.bq_table_id)

    ts_filters = so.lql_ts_filters

    period = to_timex_shift_key(so.chart_period)
    tick_count = default_period_tick_count(so.chart_period)

    utc_today = Date.utc_today()
    utc_now = DateTime.utc_now()

    partition_days =
      case so.chart_period do
        :day -> 31
        :hour -> 7
        :minute -> 1
        :second -> 1
      end

    q =
      if t? or Enum.empty?(ts_filters) do
        query
        |> where([t], t.timestamp >= lf_timestamp_sub(^utc_now, ^tick_count, ^period))
        |> where(
          partition_date() >= bq_date_sub(^utc_today, ^partition_days, "day") or
            in_streaming_buffer()
        )
        |> limit([t], ^tick_count)
      else
        %{min: min, max: max} = get_min_max_filter_timestamps(ts_filters, so.chart_period)

        query
        |> where(
          partition_date() >= ^Timex.to_date(min) and partition_date() <= ^Timex.to_date(max)
        )
        |> or_where(in_streaming_buffer())
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

  @spec parse_querystring(SO.t()) :: SO.t()
  def parse_querystring(%SO{querystring: ""} = so), do: so

  def parse_querystring(%SO{} = so) do
    schema =
      so.source
      |> Sources.Cache.get_bq_schema()

    with {:ok, lql_rules} <- Parser.parse(so.querystring, schema) do
      filter_rules = Lql.Utils.get_filter_rules(lql_rules)
      chart_rules = Lql.Utils.get_chart_rules(lql_rules)

      ts_filters = Lql.Utils.get_ts_filters(filter_rules)
      lql_meta_and_msg_filters = Lql.Utils.get_meta_and_msg_filters(filter_rules)

      %{
        so
        | lql_meta_and_msg_filters: lql_meta_and_msg_filters,
          chart_rules: chart_rules,
          lql_ts_filters: ts_filters
      }
    else
      {:error, err} ->
        halt(so, err)
    end
  end

  @spec apply_local_timestamp_correction(SO.t()) :: SO.t()
  def apply_local_timestamp_correction(%SO{} = so) do
    lql_ts_filters =
      if so.user_local_timezone do
        Enum.map(so.lql_ts_filters, fn
          %{path: "timestamp", value: value} = pvo ->
            value =
              value
              |> Timex.to_datetime(so.user_local_timezone)
              |> Timex.Timezone.convert("Etc/UTC")
              |> Timex.to_naive_datetime()

            %{pvo | value: value}
        end)
      else
        so.lql_ts_filters
      end

    %{so | lql_ts_filters: lql_ts_filters}
  end

  def apply_numeric_aggs(%SO{query: query, chart_rules: chart_rules} = so) do
    query =
      query
      |> Lql.EctoHelpers.apply_filter_rules_to_query(so.lql_meta_and_msg_filters)
      |> select_aggregates(so.chart_period)
      |> order_by([t, ...], desc: 1)

    query =
      case chart_rules do
        [%{value_type: v, path: p, aggregate: agg}]
        when v in [:integer, :float]
        when p == "timestamp" ->
          last_chart_field =
            p
            |> String.split(".")
            |> List.last()
            |> String.to_existing_atom()

          query
          |> Lql.EctoHelpers.unnest_and_join_nested_columns(:inner, p)
          |> select_merge_agg_value(agg, last_chart_field)

        [] ->
          case so.chart_data_shape_id do
            :elixir_logger_levels ->
              select_count_log_level(query)

            :cloudflare_status_codes ->
              select_count_http_status_code(query)

            nil ->
              query
              |> select_merge_log_count()
          end
      end

    query = group_by(query, 1)

    %{so | query: query}
  end

  def add_missing_agg_timestamps(%SO{} = so) do
    %{min: min, max: max} = get_min_max_filter_timestamps(so.lql_ts_filters, so.chart_period)

    if min == max do
      so
    else
      rows = intersperse_missing_range_timestamps(so.rows, min, max, so.chart_period)
      %{so | rows: rows}
    end
  end

  def intersperse_missing_range_timestamps(aggs, min, max, chart_period) do
    use Timex

    maybe_truncate_to_second = fn dt ->
      dt
      |> Timex.to_datetime()
      |> DateTime.truncate(:second)
    end

    min = maybe_truncate_to_second.(min)
    max = maybe_truncate_to_second.(max)

    {step_period, from, until} =
      case chart_period do
        :day ->
          {:days, min, max}

        :hour ->
          {:hours, %{min | second: 0, minute: 0}, %{max | second: 0, minute: 0}}

        :minute ->
          {:minutes, %{min | second: 0}, %{max | second: 0}}

        :second ->
          {:seconds, min, max}
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
          timestamp: ts,
          datetime: dt
        }
      end)

    [aggs | empty_aggs]
    |> List.flatten()
    |> Enum.uniq_by(& &1.timestamp)
    |> Enum.sort_by(& &1.timestamp, :desc)
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
