defmodule Logflare.Logs.Search do
  @moduledoc false
  alias Logflare.Google.BigQuery.{GenUtils, Query, SchemaUtils}
  alias Logflare.BigQuery.SchemaTypes
  alias Logflare.Google.BigQuery
  alias Logflare.{Source, Sources}
  alias Logflare.Repo
  alias __MODULE__.Parser
  alias Logflare.EctoQueryBQ
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
      field :searchq, String.t()
      field :partitions, {String.t(), String.t()}
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

  def search(%SearchOpts{} = opts) do
    %SearchOpts{source: %Source{token: source_id} = source} = opts

    project_id = GenUtils.get_project_id(source_id)

    {sql, params} = to_sql(opts)

    with {:ok, result} <- query_pos_params(project_id, sql, params) do
      rows = SchemaUtils.merge_rows_with_schema(result.schema, result.rows)
      {:ok, %SearchResult{rows: rows}}
    else
      err ->
        {:error, %{body: body}} = err
        body = Jason.decode!(body)
        IO.warn(hd(body["error"]["errors"])["message"])
        err
    end
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
      |> where([log], fragment("_PARTITIONTIME BETWEEN ? and ?", ^start_date, ^end_date))
      |> or_where([log], fragment("_PARTITIONTIME IS NULL"))
    else
      prune_partitions(q, opts)
    end
  end

  def filter_by_streaming_filter(q, _), do: where(q, [l], fragment("_PARTITIONTIME IS NULL"))

  def prune_partitions(q, %SearchOpts{partitions: nil}), do: q

  def prune_partitions(q, %SearchOpts{partitions: {start_date, from_date}}) do
    where(
      q,
      [log],
      fragment("_PARTITIONTIME BETWEEN ? and ?", ^start_date, ^from_date)
    )
  end

  def to_sql(%SearchOpts{} = opts) do
    import Ecto.Query
    import Ecto.Adapters.SQL, only: [to_sql: 3]

    {:ok, pathopvals} = Parser.parse(opts.searchq)
    pathopvals = EctoQueryBQ.NestedPath.to_map(pathopvals)

    query =
      opts.source.bq_table_id
      |> from(select: [:timestamp, :event_message])
      |> EctoQueryBQ.where_nesteds(pathopvals)
      # |> filter_by_injest_partitions(opts)

    {sql, params} = to_sql(:all, Repo, query)

    sql = EctoQueryBQ.ecto_pg_sql_to_bq_sql(sql)
    params = ecto_pg_params_to_bq_params(params)

    {sql, params}
  end

  def ecto_pg_params_to_bq_params(params) do
    for param <- params, do: to_bq_param(param)
  end

  def to_bq_param(param) do
    alias GoogleApi.BigQuery.V2.Model
    alias Model.QueryParameter, as: Param
    alias Model.QueryParameterType, as: Type
    alias Model.QueryParameterValue, as: Value

    param =
      case param do
        %NaiveDateTime{} -> to_string(param)
        %DateTime{} -> to_string(param)
        param -> param
      end

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
