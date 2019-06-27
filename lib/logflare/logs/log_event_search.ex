defmodule Logflare.Logs.Search do
  @moduledoc false
  alias Logflare.Google.BigQuery.{GenUtils, Query, SchemaUtils}
  alias Logflare.BigQuery.SchemaTypes
  alias Logflare.{Source, Sources}
  alias Logflare.Repo
  import Ecto.Query
  import Ecto.Adapters.SQL, only: [to_sql: 3]

  alias GoogleApi.BigQuery.V2.Api

  alias GoogleApi.BigQuery.V2.Model.{QueryRequest}

  defmodule SearchOpts do
    @moduledoc """
    Options for Logs search
    """
    use TypedStruct

    typedstruct do
      field :source, Source.t()
      field :regex, String.t()
      field :partitions, {String.t(), String.t()}
    end
  end

  defmodule SearchResult do
    @moduledoc """
    Logs search result
    """
    use TypedStruct

    typedstruct do
      field(:rows, [map()])
    end
  end

  @spec search(SearchOpts.t()) :: {:ok, SearchResult.t()} | {:error, term}
  def search(%SearchOpts{} = opts) do
    %SearchOpts{source: %Source{token: source_id}} = opts

    project_id = GenUtils.get_project_id(source_id)

    {sql, params} = to_sql(opts)

    with {:ok, result} <- query_pos_params(project_id, sql, params) do
      rows = SchemaUtils.merge_rows_with_schema(result.schema, result.rows)
      {:ok, %SearchResult{rows: rows}}
    else
      err -> err
    end
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

  def filter_by_injest_partitions(q, %SearchOpts{partitions: {start_date, end_date}} = opts) do
    if Timex.equal?(end_date, Date.utc_today()) do
      q
      |> where([log], fragment("_PARTITIONDATE BETWEEN ? and ?", ^start_date, ^end_date))
      |> or_where([log], fragment("_PARTITIONDATE IS NULL"))
    else
      prune_partitions(q, opts)
    end
  end

  def filter_by_regex_message(q, %SearchOpts{regex: regex}) do
    where(q, [log], fragment("REGEXP_CONTAINS(?, ?)", log.event_message, ^regex))
  end

  def filter_by_regex_message(q, _), do: q

  def filter_by_streaming_filter(q, _), do: where(q, [l], fragment("_PARTITIONDATE IS NULL"))

  def prune_partitions(q, %SearchOpts{partitions: {start_date, from_date}}) do
    where(
      q,
      [log],
      fragment("_PARTITIONDATE BETWEEN ? and ?", ^start_date, ^from_date)
    )
  end

  def prune_partitions(q, _), do: q

  def to_sql(%SearchOpts{} = opts) do
    import Ecto.Query
    import Ecto.Adapters.SQL, only: [to_sql: 3]

    query =
      opts.source.bq_table_id
      |> from(select: [:timestamp, :event_message])
      |> filter_by_regex_message(opts)

    {sql, params} = to_sql(:all, Repo, query)

    sql = ecto_pg_sql_to_bq_sql(sql)
    params = ecto_pg_params_to_bq_params(params)

    {sql, params}
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

  def ecto_pg_params_to_bq_params(params) do
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
end
