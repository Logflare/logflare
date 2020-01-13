defmodule Logflare.Logs.SearchOperations do
  @moduledoc false
  alias Logflare.Google.BigQuery.{GenUtils, SchemaUtils}
  alias Logflare.{Source, Sources, EctoQueryBQ}
  alias Logflare.Lql.Parser
  alias Logflare.Lql
  alias Logflare.Logs.Search.Utils
  import Ecto.Query
  import Logflare.Logs.SearchOperations.Helpers
  import Logflare.Logs.SearchQueries

  alias GoogleApi.BigQuery.V2.Api
  alias GoogleApi.BigQuery.V2.Model.QueryRequest

  use Logflare.GenDecorators
  @decorate_all pass_through_on_error_field()

  @default_limit 100
  @default_processed_bytes_limit 10_000_000_000

  # Note that this is only a timeout for the request, not the query.
  # If the query takes longer to run than the timeout value, the call returns without any results and with the 'jobComplete' flag set to false.
  @query_request_timeout 60_000

  defmodule SearchOperation do
    @moduledoc """
    Logs search options and result
    """
    use TypedStruct

    typedstruct do
      field :source, Source.t()
      field :querystring, String.t()
      field :query, Ecto.Query.t()
      field :query_result, term()
      field :sql_params, {term(), term()}
      field :tailing?, boolean
      field :tailing_initial?, boolean
      field :rows, [map()], default: []
      field :filter_rules, [map()], default: []
      field :chart_rules, [map()], default: []
      field :error, term()
      field :stats, :map
      field :use_local_time, boolean
      field :user_local_timezone, String.t()
      field :search_chart_period, atom()
      field :search_chart_aggregate, atom(), default: :avg
      field :timestamp_truncator, term()
    end
  end

  alias SearchOperation, as: SO

  @spec do_query(SO.t()) :: SO.t()
  def do_query(%SO{} = so) do
    %SO{source: %Source{token: source_id}} = so
    project_id = GenUtils.get_project_id(source_id)
    conn = GenUtils.get_conn()

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

    result =
      Api.Jobs.bigquery_jobs_query(
        conn,
        project_id,
        body: dry_run
      )

    with {:ok, response} <- result,
         is_within_limit? =
           String.to_integer(response.totalBytesProcessed) <= @default_processed_bytes_limit,
         {:total_bytes_processed, true} <- {:total_bytes_processed, is_within_limit?} do
      Api.Jobs.bigquery_jobs_query(
        conn,
        project_id,
        body: query_request
      )
    else
      {:total_bytes_processed, false} ->
        {:error,
         "Query halted: total bytes processed for this query is expected to be larger than #{
           div(@default_processed_bytes_limit, 1_000_000_000)
         } GB"}

      errtup ->
        errtup
    end
    |> Utils.put_result_in(so, :query_result)
    |> prepare_query_result()
  end

  @spec prepare_query_result(SO.t()) :: SO.t()
  def prepare_query_result(%SO{} = so) do
    query_result =
      so.query_result
      |> Map.update(:totalBytesProcessed, 0, &Utils.maybe_string_to_integer/1)
      |> Map.update(:totalRows, 0, &Utils.maybe_string_to_integer/1)
      |> AtomicMap.convert(%{safe: false})

    %{so | query_result: query_result}
  end

  @spec order_by_default(SO.t()) :: SO.t()
  def order_by_default(%SO{} = so) do
    %{so | query: order_by(so.query, desc: :timestamp)}
  end

  @spec apply_limit_to_query(SO.t()) :: SO.t()
  def apply_limit_to_query(%SO{} = so) do
    %{so | query: limit(so.query, @default_limit)}
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
    rows = SchemaUtils.merge_rows_with_schema(schema, rows)

    %{so | rows: rows}
  end

  @spec default_from(SO.t()) :: SO.t()
  def default_from(%SO{} = so) do
    %{so | query: from(so.source.bq_table_id)}
  end

  @spec apply_to_sql(SO.t()) :: SO.t()
  def apply_to_sql(%SO{} = so) do
    %{so | sql_params: EctoQueryBQ.SQL.to_sql(so.query)}
  end

  @spec apply_wheres(SO.t()) :: SO.t()
  def apply_wheres(%SO{} = so) do
    %{so | query: Lql.EctoHelpers.apply_filter_rules_to_query(so.query, so.filter_rules)}
  end

  @spec parse_querystring(SO.t()) :: SO.t()
  def parse_querystring(%SO{} = so) do
    schema =
      so.source
      |> Sources.Cache.get_bq_schema()

    with {:ok, lql_rules} <- Parser.parse(so.querystring, schema) do
      filter_rules = Lql.Utils.get_filter_rules(lql_rules)
      chart_rules = Lql.Utils.get_chart_rules(lql_rules)
      %{so | filter_rules: filter_rules, chart_rules: chart_rules}
    else
      err ->
        Utils.put_result_in(err, so, :filter_rules)
    end
  end

  @spec events_partition_or_streaming(SO.t()) :: SO.t()
  def events_partition_or_streaming(%SO{tailing?: true, tailing_initial?: true} = so) do
    so
    |> Map.update!(:query, &where_default_tailing_events_partition/1)
    |> drop_timestamp_filter_rules
  end

  def events_partition_or_streaming(%SO{tailing?: true} = so) do
    so
    |> Map.update!(:query, &where_streaming_buffer/1)
    |> drop_timestamp_filter_rules
  end

  def events_partition_or_streaming(%SO{} = so) do
    partition_filter_rules =
      for %{path: "timestamp", operator: op, value: v} <- so.filter_rules do
        op =
          case op do
            :> -> :>=
            :< -> :<=
            :<= -> :<=
            :>= -> :>=
          end

        %{path: "_PARTITIONDATE", operator: op, value: Timex.to_date(v)}
      end

    %{so | filter_rules: so.filter_rules ++ partition_filter_rules}
  end

  @spec drop_timestamp_filter_rules(SO.t()) :: SO.t()
  def drop_timestamp_filter_rules(%SO{} = so) do
    %{so | filter_rules: Enum.reject(so.filter_rules, &(&1.path === "timestamp"))}
  end

  @spec verify_path_in_schema(SO.t()) :: SO.t()
  def verify_path_in_schema(%SO{} = so) do
    flatmap =
      so.source
      |> Sources.Cache.get_bq_schema()
      |> Logflare.Logs.Validators.BigQuerySchemaChange.to_typemap()
      |> Iteraptor.to_flatmap()
      |> Enum.map(fn {k, v} -> {String.replace(k, ".fields.", "."), v} end)
      |> Enum.map(fn {k, _} -> String.trim_trailing(k, ".t") end)

    result =
      Enum.reduce_while(so.filter_rules, :ok, fn %{path: path}, _ ->
        if path in flatmap do
          {:cont, :ok}
        else
          {:halt, {:error, "#{path} not present in source schema"}}
        end
      end)

    so = Utils.put_result_in(result, so)

    if not Enum.empty?(so.chart_rules) && hd(so.chart_rules).path not in flatmap do
      Utils.put_result_in(
        {:error, "chart field #{so.chart.path} not present in source schema"},
        so
      )
    else
      so
    end
  end

  @spec apply_local_timestamp_correction(SO.t()) :: SO.t()
  def apply_local_timestamp_correction(%SO{} = so) do
    filter_rules =
      Enum.map(so.filter_rules, fn
        %{path: "timestamp", value: value} = pvo ->
          if so.user_local_timezone do
            value =
              value
              |> Timex.to_datetime(so.user_local_timezone)
              |> Timex.Timezone.convert("Etc/UTC")
              |> Timex.to_naive_datetime()

            %{pvo | value: value}
          end

        pvo ->
          pvo
      end)

    %{so | filter_rules: filter_rules}
  end

  @spec apply_select_all_schema(SO.t()) :: SO.t()
  def apply_select_all_schema(%SO{} = so) do
    top_level_fields =
      so.source
      |> Sources.Cache.get_bq_schema()
      |> Logflare.Logs.Validators.BigQuerySchemaChange.to_typemap()
      |> Map.keys()

    %{so | query: select(so.query, ^top_level_fields)}
  end

  @spec apply_group_by_timestamp_period(SO.t()) :: SO.t()
  def apply_group_by_timestamp_period(%SO{} = so) do
    group_by = [
      timestamp_truncator(so.search_chart_period)
    ]

    query = group_by(so.query, 1)
    %{so | query: query}
  end

  @spec exclude_limit(SO.t()) :: SO.t()
  def exclude_limit(%SO{} = so) do
    %{so | query: Ecto.Query.exclude(so.query, :limit)}
  end

  @spec apply_numeric_aggs(SO.t()) :: SO.t()
  def apply_numeric_aggs(%SO{chart_rules: [%{value: chart_value}]} = so)
      when chart_value not in ~w[integer float]a do
    result =
      {:error,
       "Error: can't aggregate on a non-numeric field type '#{chart_value}'. Check the schema for the field used with chart operator."}

    Utils.put_result_in(result, so)
  end

  @spec apply_numeric_aggs(SO.t()) :: SO.t()
  def apply_numeric_aggs(%SO{query: query, chart_rules: chart = [%{value: chart_value}]} = so)
      when chart_value in ~w[integer float]a do
    timestamp_filter_rules = Enum.filter(so.filter_rules, &(&1.path === "timestamp"))

    last_chart_field =
      so.chart.path
      |> String.split(".")
      |> List.last()
      |> String.to_existing_atom()

    {min, max} =
      if so.tailing? or Enum.empty?(timestamp_filter_rules) do
        default_min_max_timestamps_for_chart_period(so.search_chart_period)
      else
        timestamp_filter_rules
        |> override_min_max_for_open_intervals()
        |> min_max_timestamps()
      end

    query =
      if so.tailing? do
        query
        |> where_default_tailing_charts_partition(so.search_chart_period)
        |> where_timestamp_tailing(min, max)
      else
        query
        |> where_partitiondate_between(min, max)
      end

    query =
      query
      |> select_timestamp(so.search_chart_period)
      |> Lql.EctoHelpers.apply_filter_rules_to_query(chart)
      |> limit_aggregate_chart_period(so.search_chart_period)
      |> select_agg_value(so.search_chart_aggregate, last_chart_field)
      |> order_by([t, ...], desc: 1)
      |> join_missing_range_timestamps(min, max, so.search_chart_period)
      |> select([t, ts], %{
        timestamp: coalesce(t.timestamp, ts.timestamp),
        value: coalesce(t.value, ts.value)
      })
      |> order_by([t], desc: 1)

    %{so | query: query}
  end

  def apply_numeric_aggs(%SO{chart_rules: []} = so) do
    query = so.query
    timestamp_filter_rules = Enum.filter(so.filter_rules, &(&1.path === "timestamp"))

    {min, max} =
      if so.tailing? or Enum.empty?(timestamp_filter_rules) do
        default_min_max_timestamps_for_chart_period(so.search_chart_period)
      else
        timestamp_filter_rules
        |> override_min_max_for_open_intervals()
        |> min_max_timestamps()
      end

    query =
      if so.tailing? do
        query
        |> where_default_tailing_charts_partition(so.search_chart_period)
        |> where_timestamp_tailing(min, max)
      else
        query
        |> where_partitiondate_between(min, max)
      end

    query =
      query
      |> select_timestamp(so.search_chart_period)
      |> select_merge([c, ...], %{
        value: count(c.timestamp)
      })
      |> order_by([t, ...], desc: 1)
      |> limit_aggregate_chart_period(so.search_chart_period)
      |> join_missing_range_timestamps(min, max, so.search_chart_period)
      |> select([t, ts], %{
        timestamp: coalesce(t.timestamp, ts.timestamp),
        value: coalesce(t.value, ts.value)
      })
      |> order_by([t], desc: 1)

    %{so | query: query}
  end

  @spec process_agg_query_result(SO.t()) :: SO.t()
  def process_agg_query_result(%SO{} = so) do
    %{schema: schema, rows: rows} = so.query_result
    rows = SchemaUtils.merge_rows_with_schema(schema, rows)

    rows =
      rows
      |> format_agg_row_keys()
      |> format_agg_row_values()

    %{so | rows: rows}
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
