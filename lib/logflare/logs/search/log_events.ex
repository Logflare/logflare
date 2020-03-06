defmodule Logflare.Logs.LogEvents do
  @moduledoc false
  alias Logflare.LogEvent
  alias Logflare.Logs.SearchOperations
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.BqRepo

  def get_event_by_id_and_timestamp(source, kw) do
    id = kw[:id]
    timestamp = kw[:timestamp]
    query = SearchOperations.log_event_query(source, id, timestamp)

    %{sql_with_params: sql_with_params, params: params} =
      SearchOperations.Helpers.ecto_query_to_sql(query, source)

    {:ok, %{schema: schema, rows: rows}} =
      BqRepo.query(source.bq_project_id, sql_with_params, params)

    schema
    |> SchemaUtils.merge_rows_with_schema(rows)
    |> Enum.map(&MapKeys.to_atoms_unsafe!/1)
    |> hd
    |> LogEvent.make_from_db(%{source: source})
  end
end
