defmodule Logflare.Logs.Search do
  @moduledoc false
  alias Logflare.Google.BigQuery.{GenUtils, Query, SchemaUtils}
  alias Logflare.{Source, Sources}
  alias Logflare.Repo

  alias GoogleApi.BigQuery.V2.Api

  alias GoogleApi.BigQuery.V2.Model.{
    QueryRequest,
    QueryParameter,
    QueryParameterType,
    QueryParameterValue
  }

  defmodule SearchOpts do
    @moduledoc """
    Options for Logs search
    """
    use TypedStruct

    typedstruct do
      field :source, Source.t()
      field :regex, String.t()
    end
  end

  defmodule SearchResult do
    @moduledoc """
    Logs search result
    """
    use TypedStruct

    typedstruct do
      field :rows, [map()]
    end
  end

  @spec search(SearchOpts.t()) :: {:ok, SearchResult.t()} | {:error, term}
  def search(%SearchOpts{} = opts) do
    %SearchOpts{regex: regex, source: %Source{bq_table_id: bq_table_id} = source} = opts
    source_id = source.token
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
      query(
        project_id,
        sql,
        [
          %QueryParameter{
            name: "regex",
            parameterType: %QueryParameterType{type: "STRING"},
            parameterValue: %QueryParameterValue{value: regex}
          }
        ]
      )

    rows = SchemaUtils.merge_rows_with_schema(result.schema, result.rows)

    {:ok,
     %SearchResult{
       rows: rows
     }}
  end

  def query(project_id, sql, params) do
    conn = GenUtils.get_conn()

    Api.Jobs.bigquery_jobs_query(
      conn,
      project_id,
      body: %QueryRequest{
        query: sql,
        useLegacySql: false,
        useQueryCache: true,
        parameterMode: "NAMED",
        queryParameters: params
      }
    )
  end

  def to_sql(%SearchOpts{} = opts) do
    import Ecto.Query
    import  Ecto.Adapters.SQL, only: [to_sql: 3]

    q =
      from opts.source.bq_table_id,
        select: [:timestamp, :event_message]

    {sql, params} = to_sql(:all, Repo, q)

  def ecto_pg_sql_to_bq_sql(sql) do
    sql
    # replaces PG-style to BQ-style positional parameters
    |> String.replace(~r/\$\d/, "?")
    # removes double quotes around the names after the dot
    |> String.replace(~r/\."(\w+)"/, ".\\1")
    # removes double quotes around the qualified BQ table id
    |> String.replace(~r/FROM\s+"(.+)"/, "FROM \\1")
  end
end
