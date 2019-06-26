defmodule Logflare.Logs.Search do
  @moduledoc false
  alias Logflare.Google.BigQuery.{GenUtils, Query, SchemaUtils}
  alias Logflare.BigQuery.SchemaTypes
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
    %SearchOpts{ source: %Source{ bq_table_id: bq_table_id, token: source_id } } = opts
    project_id = GenUtils.get_project_id(source_id)

    {sql, params} = to_sql(opts)

    {:ok, result} =
      query_pos_params(
        project_id,
        sql,
        build_bq_pos_params(params)
      )

    rows = SchemaUtils.merge_rows_with_schema(result.schema, result.rows)

    {:ok, %SearchResult{ rows: rows }}
  end

  def query_named_params(project_id, sql, params) do
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

  def query_pos_params(project_id, sql, params) do
    conn = GenUtils.get_conn()

    Api.Jobs.bigquery_jobs_query(
      conn,
      project_id,
      body: %QueryRequest{
        query: sql,
        useLegacySql: false,
        useQueryCache: true,
        parameterMode: "POSITIONAL",
        queryParameters: params
      }
    )
  end

  def to_sql(%SearchOpts{} = opts) do
    import Ecto.Query
    import Ecto.Adapters.SQL, only: [to_sql: 3]

    q =
      from(opts.source.bq_table_id, select: [:timestamp, :event_message])

    q =
      if opts.regex do
        where(q, [log], fragment("REGEXP_CONTAINS(?, ?)", log.event_message, ^opts.regex))
      else
        q
      end

    {sql, params} = to_sql(:all, Repo, q)

    sql =
      ecto_pg_sql_to_bq_sql(sql)

    {sql, params}
  end

  def build_bq_pos_params(params) when is_list(params) do
    alias GoogleApi.BigQuery.V2.Model
    alias Model.QueryParameter, as: Param
    alias Model.QueryParameterType, as: Type
    alias Model.QueryParameterValue, as: Value

    for param <- params do
      %Param{
        parameterType: %Type{
          type: SchemaTypes.to_schema_type(param)
        },
        parameterValue: %Value{
          value: param
        }
      }
    end
  end

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
