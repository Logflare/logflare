defmodule Logflare.Logs.Search do
  @moduledoc false
  alias Logflare.Google.BigQuery.{GenUtils, Query, SchemaUtils}
  alias Logflare.{Source, Sources}

  def utc_today(%{source: %Source{} = source}) do
    source_id = source.token
    conn = GenUtils.get_conn()
    project_id = GenUtils.get_project_id(source_id)
    table = GenUtils.format_table_name(source_id)

    sql = ~s|
    SELECT timestamp, event_message, metadata
      FROM `#{project_id}`.#{source.user_id}_dev.`#{table}`
      WHERE true
      AND _PARTITIONTIME >= "#{Date.utc_today()}"
    UNION ALL
    SELECT
      timestamp, event_message, metadata
      FROM `#{project_id}`.#{source.user_id}_dev.`#{table}`
    WHERE true
      AND _PARTITIONTIME IS NULL
    ORDER BY timestamp DESC
    LIMIT 100
    |

    {:ok, result} =
      Query.query(
        conn,
        project_id,
        sql
      )

    SchemaUtils.merge_rows_with_schema(result.schema, result.rows)
  end
end
