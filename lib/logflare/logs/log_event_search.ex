defmodule Logflare.Logs.Search do
  @moduledoc false
  alias Logflare.Google.BigQuery.{GenUtils, Query, SchemaUtils}
  alias Logflare.{Source, Sources}
  alias GoogleApi.BigQuery.V2.Model.{QueryParameter, QueryParameterType, QueryParameterValue}

  @spec utc_today(%{regex: String.t(), source: Logflare.Source.t()}) ::
          {:ok, %{result: nil | [any]}}
  def utc_today(%{regex: regex, source: %Source{bq_table_id: bq_table_id} = source}) do

    source_id = source.token
    conn = GenUtils.get_conn()
    project_id = GenUtils.get_project_id(source_id)

    sql = ~s|
    SELECT timestamp, event_message
      FROM #{bq_table_id}
    WHERE true
      AND _PARTITIONTIME >= "#{Date.utc_today()}"
      AND REGEXP_CONTAINS(event_message, @regex)
    UNION ALL
    SELECT
      timestamp, event_message
      FROM #{bq_table_id}
    WHERE true
      AND _PARTITIONTIME IS NULL
      AND REGEXP_CONTAINS(event_message, @regex)
    ORDER BY timestamp DESC
    LIMIT 100
    |

    {:ok, result} =
      Query.query(
        conn,
        project_id,
        sql,
        [%QueryParameter{
          name: "regex",
          parameterType: %QueryParameterType{type: "STRING"},
          parameterValue: %QueryParameterValue{value: regex}
        }])

    {:ok,
     %{
       result: SchemaUtils.merge_rows_with_schema(result.schema, result.rows)
     }}
  end
end
