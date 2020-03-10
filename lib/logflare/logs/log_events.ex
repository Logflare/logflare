defmodule Logflare.Logs.LogEvents do
  @moduledoc false
  alias Logflare.Google.BigQuery.GCPConfig
  alias Logflare.LogEvent
  alias Logflare.Logs.SearchOperations
  alias Logflare.Logs.SearchQueries
  alias Logflare.Source
  alias Logflare.BqRepo

  @spec fetch_event_by_id_and_timestamp(Source.t(), Keyword) :: {:ok, map()} | {:error, map()}
  def fetch_event_by_id_and_timestamp(source, kw) do
    id = kw[:id]
    timestamp = kw[:timestamp]
    query = SearchQueries.log_event_query(source, id, timestamp)

    %{sql_with_params: sql_with_params, params: params} =
      SearchOperations.Helpers.ecto_query_to_sql(query, source)

    bq_project_id = source.user.bigquery_project_id || GCPConfig.default_project_id()

    with {:ok, %{rows: [row]}} <- BqRepo.query(bq_project_id, sql_with_params, params) do
      IO.inspect(row)
      le = LogEvent.make_from_db(row, %{source: source})
      {:ok, le}
    else
      errtup -> errtup
    end
  end
end
