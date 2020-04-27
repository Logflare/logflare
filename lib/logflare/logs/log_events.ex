defmodule Logflare.Logs.LogEvents do
  @moduledoc false
  alias Logflare.Google.BigQuery.GCPConfig
  alias Logflare.Sources
  alias Logflare.LogEvent
  alias Logflare.Logs.SearchOperations
  alias Logflare.Logs.SearchQueries
  alias Logflare.BqRepo

  @spec fetch_event_by_id_and_timestamp(atom, keyword) :: {:ok, map()} | {:error, map()}
  def fetch_event_by_id_and_timestamp(source_token, kw) when is_atom(source_token) do
    id = kw[:id]
    timestamp = kw[:timestamp]
    source = Sources.Cache.get_by_id_and_preload(source_token)
    bq_table_id = source.bq_table_id
    query = SearchQueries.source_log_event_query(bq_table_id, id, timestamp)

    %{sql_with_params: sql_with_params, params: params} =
      SearchOperations.Helpers.ecto_query_to_sql(query, source)

    bq_project_id = source.user.bigquery_project_id || GCPConfig.default_project_id()

    query_result = BqRepo.query_with_sql_and_params(bq_project_id, sql_with_params, params)

    with {:ok, result} <- query_result do
      case result do
        %{rows: []} ->
          {:error, :not_found}

        %{rows: [row]} ->
          le = LogEvent.make_from_db(row, %{source: source})
          {:ok, le}

        %{rows: rows} when length(rows) >= 1 ->
          row = Enum.find(rows, &(&1.id == id))
          le = LogEvent.make_from_db(row, %{source: source})
          {:ok, le}
      end
    else
      errtup -> errtup
    end
  end
end
