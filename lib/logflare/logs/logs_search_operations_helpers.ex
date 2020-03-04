defmodule Logflare.Logs.SearchOperations.Helpers do
  @moduledoc false
  alias Logflare.Lql.FilterRule
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.EctoQueryBQ
  alias Logflare.Source
  alias GoogleApi.BigQuery.V2.Model.QueryRequest
  alias Logflare.Google.BigQuery.{GenUtils, SchemaUtils}
  alias Logflare.Google.BigQuery
  alias Logflare.{Source, Sources, EctoQueryBQ}
  @query_request_timeout 60_000

  def get_min_max_filter_timestamps(timestamp_filter_rules, chart_period) do
    if Enum.empty?(timestamp_filter_rules) do
      default_min_max_for_tailing_chart_period(chart_period)
    else
      timestamp_filter_rules
      |> override_min_max_for_open_intervals(chart_period)
      |> min_max_timestamps()
    end
  end

  def default_min_max_for_tailing_chart_period(period) when is_atom(period) do
    shift_interval =
      case period do
        :day -> [days: -31 + 1]
        :hour -> [hours: -168 + 1]
        :minute -> [minutes: -120 + 1]
        :second -> [seconds: -180 + 1]
      end

    {Timex.shift(Timex.now(), shift_interval), Timex.now()}
  end

  def min_max_timestamps(timestamps) do
    Enum.min_max_by(timestamps, &Timex.to_unix/1)
  end

  defp override_min_max_for_open_intervals([%{operator: op, value: ts}], period)
       when op in ~w[> >=]a do
    shift =
      case period do
        :day -> [days: 365]
        :hour -> [hours: 480]
        :minute -> [minutes: 360]
        :second -> [seconds: 300]
      end

    max = ts |> Timex.shift(shift)

    max =
      if Timex.compare(max, Timex.now()) > 0 do
        Timex.now()
      else
        max
      end

    [ts, max]
  end

  defp override_min_max_for_open_intervals([%{operator: op, value: ts}], period)
       when op in ~w[< <=]a do
    shift =
      case period do
        :day -> [days: -365]
        :hour -> [hours: -480]
        :minute -> [minutes: -360]
        :second -> [seconds: -300]
      end

    [ts |> Timex.shift(shift), ts]
  end

  defp override_min_max_for_open_intervals(filter_rules, _) do
    Enum.map(filter_rules, & &1.value)
  end

  def convert_timestamp_timezone(row, user_timezone) do
    Map.update!(row, "timestamp", &Timex.Timezone.convert(&1, user_timezone))
  end

  def ecto_query_to_sql(%Ecto.Query{} = query, %Source{} = source) do
    %{bigquery_dataset_id: bq_dataset_id} = GenUtils.get_bq_user_info(source.token)
    {sql, params} = EctoQueryBQ.SQL.to_sql_params(query)
    sql_and_params = {EctoQueryBQ.SQL.substitute_dataset(sql, bq_dataset_id), params}
    sql_string = EctoQueryBQ.SQL.sql_params_to_sql(sql_and_params)
    %{sql_with_params: sql, params: params, sql_string: sql_string}
  end

  def execute_query(sql, params) when is_binary(sql) and is_list(params) do
    query_request = %QueryRequest{
      query: sql,
      useLegacySql: false,
      useQueryCache: true,
      parameterMode: "POSITIONAL",
      queryParameters: params,
      dryRun: false,
      timeoutMs: @query_request_timeout
    }

    with {:ok, response} <- BigQuery.query(query_request) do
      AtomicMap.convert(response, %{safe: false})
    else
      errtup -> errtup
    end
  end
end
