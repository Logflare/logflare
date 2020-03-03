defmodule Logflare.Logs.SearchOperations do
  @moduledoc false
  alias Logflare.Google.BigQuery.{GenUtils, SchemaUtils}
  alias Logflare.Google.BigQuery
  alias Logflare.{Source, Sources, EctoQueryBQ}
  alias Logflare.Lql.Parser
  alias Logflare.Lql
  alias Logflare.Logs.Search.Utils
  alias Logflare.Lql.{ChartRule, FilterRule}
  import Ecto.Query
  import Logflare.Logs.SearchOperations.Helpers
  import Logflare.Logs.SearchQueries

  alias GoogleApi.BigQuery.V2.Model.QueryRequest

  alias Logflare.Ecto.BQQueryAPI
  import BQQueryAPI.UDF
  import BQQueryAPI

  use Logflare.GenDecorators

  @default_limit 100
  @default_processed_bytes_limit 10_000_000_000

  # Note that this is only a timeout for the request, not the query.
  # If the query takes longer to run than the timeout value, the call returns without any results and with the 'jobComplete' flag set to false.
  @query_request_timeout 60_000

  # Halt reasons

  @timestamp_filter_with_tailing "Timestamp filters can't be used if live tail search is active"

  defmodule SearchOperation do
    @moduledoc """
    Logs search options and result
    """
    use TypedStruct

    typedstruct do
      field :source, Source.t()
      field :querystring, String.t(), enforce: true
      field :query, Ecto.Query.t()
      field :query_result, term()
      field :sql_params, {term(), term()}
      field :sql_string, String.t()
      field :tailing?, boolean, enforce: true
      field :tailing_initial?, boolean
      field :rows, [map()], default: []
      field :filter_rules, [FilterRule.t()], default: []
      field :chart_rules, [ChartRule.t()], default: []
      field :error, term()
      field :stats, :map
      field :use_local_time, boolean
      field :user_local_timezone, String.t()
      field :chart_period, atom(), default: :minute, enforce: true
      field :chart_aggregate, atom(), default: :count, enforce: true
      field :chart_data_shape_id, atom(), default: nil, enforce: true
      field :type, :events | :aggregates
    end
  end

  alias SearchOperation, as: SO

  @spec do_query(SO.t()) :: SO.t()
  def do_query(%SO{} = so) do
    {sql, params} = so.sql_params

    query_request = %QueryRequest{
      query: sql,
      useLegacySql: false,
      useQueryCache: true,
      parameterMode: "POSITIONAL",
      queryParameters: params,
      dryRun: false,
      timeoutMs: @query_request_timeout
    }

    dry_run = %{query_request | dryRun: true}

    with {:ok, response} <- BigQuery.query(dry_run),
         is_within_limit? =
           String.to_integer(response.totalBytesProcessed) <= @default_processed_bytes_limit,
         {:total_bytes_processed, true} <- {:total_bytes_processed, is_within_limit?},
         {:ok, result} = BigQuery.query(query_request) do
      result
      |> Map.update(:totalBytesProcessed, 0, &Utils.maybe_string_to_integer/1)
      |> Map.update(:totalRows, 0, &Utils.maybe_string_to_integer/1)
      |> AtomicMap.convert(%{safe: false})
      |> Utils.put_result_in(so, :query_result)
    else
      {:total_bytes_processed, false} ->
        {:error,
         "Query halted: total bytes processed for this query is expected to be larger than #{
           div(@default_processed_bytes_limit, 1_000_000_000)
         } GB"}
        |> Utils.put_result_in(so, :query_result)

      errtup ->
        Utils.put_result_in(errtup, so, :query_result)
    end
  end

  @spec order_by_default(SO.t()) :: SO.t()
  def order_by_default(%SO{} = so) do
    %{so | query: order_by(so.query, desc: :timestamp)}
  end

  @spec apply_limit_to_query(SO.t()) :: SO.t()
  def apply_limit_to_query(%SO{} = so) do
    %{so | query: limit(so.query, @default_limit)}
  end

  @spec apply_halt_conditions(SO.t()) :: SO.t()
  def apply_halt_conditions(%SO{} = so) do
    cond do
      so.tailing? and Enum.find(so.filter_rules, &(&1.path == "timestamp")) ->
        so
        |> Utils.put_result({:error, :halted})
        |> Utils.put_status(:halted, @timestamp_filter_with_tailing)

      true ->
        so
    end
  end

  def put_chart_data_shape_id(%SO{} = so) do
    bq_schema = Sources.Cache.get_bq_schema(so.source)
    flat_type_map = Lql.Utils.bq_schema_to_flat_typemap(bq_schema)

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

  @spec put_stats(SO.t()) :: SO.t()
  def put_stats(%SO{} = so) do
    stats =
      so.stats
      |> Map.merge(%{
        total_rows: so.query_result.total_rows,
        total_bytes_processed: so.query_result.total_bytes_processed
      })
      |> Map.put(
        :total_duration,
        System.monotonic_time(:millisecond) - so.stats.start_monotonic_time
      )

    %{so | stats: stats}
  end

  @spec process_query_result(SO.t()) :: SO.t()
  def process_query_result(%SO{} = so) do
    %{schema: schema, rows: rows} = so.query_result

    rows =
      schema
      |> SchemaUtils.merge_rows_with_schema(rows)
      |> Enum.map(&MapKeys.to_atoms_unsafe!/1)

    %{so | rows: rows}
  end

  @spec process_query_result(SO.t(), :aggs | :events) :: SO.t()
  def process_query_result(%SO{} = so, :aggs) do
    %{schema: schema, rows: rows} = so.query_result

    rows =
      schema
      |> SchemaUtils.merge_rows_with_schema(rows)
      |> Enum.map(&MapKeys.to_atoms_unsafe!/1)
      |> Enum.map(fn agg ->
        Map.put(agg, :datetime, Timex.from_unix(agg.timestamp, :microsecond))
      end)

    %{so | rows: rows}
  end

  def apply_timestamp_filter_rules(%SO{tailing?: t?, tailing_initial?: ti?, type: :events} = so) do
    so = %{so | query: from(so.source.bq_table_id)}
    query = so.query

    utc_today = Date.utc_today()
    utc_now = DateTime.utc_now()

    ts_filters = Enum.filter(so.filter_rules, &(&1.path == "timestamp"))

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
          {min, max} = get_min_max_filter_timestamps(ts_filters, so.chart_period)

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
    so = %{so | query: from(so.source.bq_table_id)}
    query = so.query

    ts_filters = Enum.filter(so.filter_rules, &(&1.path == "timestamp"))

    [{period, number}] =
      case so.chart_period do
        :day -> [days: 31]
        :hour -> [hours: 168]
        :minute -> [minutes: 120]
        :second -> [seconds: 180]
      end

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
      cond do
        t? ->
          query
          |> where([t], t.timestamp >= lf_timestamp_sub(^utc_now, ^number, ^period))
          |> where(
            partition_date() >= bq_date_sub(^utc_today, ^partition_days, "day") or
              in_streaming_buffer()
          )
          |> limit([t], ^number)

        not t? && Enum.empty?(ts_filters) ->
          query
          |> where([t], t.timestamp >= lf_timestamp_sub(^utc_today, ^number, ^period))
          |> where(
            partition_date() >= bq_date_sub(^utc_today, ^partition_days, "day") or
              in_streaming_buffer()
          )
          |> limit([t], ^number)

        not Enum.empty?(ts_filters) ->
          {min, max} = get_min_max_filter_timestamps(ts_filters, so.chart_period)

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
    q = Lql.EctoHelpers.apply_filter_rules_to_query(q, so.filter_rules)

    %{so | query: q}
  end

  @spec apply_to_sql(SO.t()) :: SO.t()
  def apply_to_sql(%SO{} = so) do
    %{bigquery_dataset_id: bq_dataset_id} = GenUtils.get_bq_user_info(so.source.token)
    {sql, params} = EctoQueryBQ.SQL.to_sql_params(so.query)
    sql_and_params = {EctoQueryBQ.SQL.substitute_dataset(sql, bq_dataset_id), params}
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
      %{so | filter_rules: filter_rules, chart_rules: chart_rules}
    else
      err -> Utils.put_result_in(err, so, :filter_rules)
    end
  end

  @spec apply_local_timestamp_correction(SO.t()) :: SO.t()
  def apply_local_timestamp_correction(%SO{} = so) do
    filter_rules =
      if so.user_local_timezone do
        Enum.map(so.filter_rules, fn
          %{path: "timestamp", value: value} = pvo ->
            value =
              value
              |> Timex.to_datetime(so.user_local_timezone)
              |> Timex.Timezone.convert("Etc/UTC")
              |> Timex.to_naive_datetime()

            %{pvo | value: value}

          pvo ->
            pvo
        end)
      else
        so.filter_rules
      end

    %{so | filter_rules: filter_rules}
  end

  @spec apply_select_all_schema(SO.t()) :: SO.t()
  def apply_select_all_schema(%SO{} = so) do
    top_level_fields =
      so.source
      |> Sources.Cache.get_bq_schema()
      |> SchemaUtils.to_typemap()
      |> Map.keys()

    %{so | query: select(so.query, ^top_level_fields)}
  end

  @spec apply_numeric_aggs(SO.t()) :: SO.t()
  def apply_numeric_aggs(%SO{chart_rules: [%ChartRule{value_type: vt, path: p}]} = so)
      when vt not in ~w[integer float]a do
    msg =
      "Error: can't aggregate on a non-numeric field type '#{vt}' for path #{p}. Check the source schema for the field used with chart operator."

    Utils.put_result_in({:error, msg}, so)
  end

  def apply_numeric_aggs(%SO{query: query, chart_rules: chart_rules} = so) do
    {ts_filter_rules, filter_rules} = Enum.split_with(so.filter_rules, &(&1.path == "timestamp"))

    query =
      query
      |> Lql.EctoHelpers.apply_filter_rules_to_query(filter_rules)
      |> select_aggregates(so.chart_period)
      |> order_by([t, ...], desc: 1)

    query =
      case chart_rules do
        [%{value_type: chart_value, path: chart_path}] when chart_value in [:integer, :float] ->
          last_chart_field =
            chart_path
            |> String.split(".")
            |> List.last()
            |> String.to_existing_atom()

          query
          |> Lql.EctoHelpers.unnest_and_join_nested_columns(:inner, chart_path)
          |> select_merge_agg_value(so.chart_aggregate, last_chart_field)

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

    query =
      query
      |> group_by(1)

    %{so | query: query}
  end

  def add_missing_agg_timestamps(%SO{} = so) do
    {ts_filter_rules, _filter_rules} = Enum.split_with(so.filter_rules, &(&1.path == "timestamp"))

    {min, max} = get_min_max_filter_timestamps(ts_filter_rules, so.chart_period)

    if min == max do
      so
    else
      %{so | rows: intersperse_missing_range_timestamps(so.rows, min, max, so.chart_period)}
    end
  end

  def intersperse_missing_range_timestamps(aggs, min, max, chart_period) do
    use Timex

    maybe_truncate_to_second = fn dt ->
      if match?(%DateTime{}, dt) do
        DateTime.truncate(dt, :second)
      else
        dt
      end
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
    |> Enum.sort_by(& &1.timestamp)
    |> Enum.reverse()
  end

  @spec put_time_stats(SO.t()) :: SO.t()
  def put_time_stats(%SO{} = so) do
    so
    |> Map.put(
      :stats,
      %{
        start_monotonic_time: System.monotonic_time(:millisecond),
        total_duration: nil
      }
    )
  end
end
