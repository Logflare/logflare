defmodule Logflare.Logs.Search do
  @moduledoc false
  alias Logflare.Google.BigQuery.{GenUtils, Query, SchemaUtils}
  alias Logflare.Google.BigQuery
  alias Logflare.{Source, Sources}
  alias Logflare.Logs.Search.Parser
  alias Logflare.EctoQueryBQ
  alias Logflare.Repo
  import Ecto.Query

  alias GoogleApi.BigQuery.V2.Api
  alias GoogleApi.BigQuery.V2.Model.QueryRequest

  defmodule SearchOpts do
    @moduledoc """
    Options for Logs search
    """
    use TypedStruct

    typedstruct do
      field :source, Source.t()
      field :query, String.t()
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

  def search(%SearchOpts{source: %Source{token: source_id}} = opts) do
    project_id = GenUtils.get_project_id(source_id)

    {:ok, pathopvals} = Parser.parse(opts.query)

    {sql, params} =
      opts.source.bq_table_id
      |> from(select: [:timestamp, :event_message, :metadata])
      |> EctoQueryBQ.where_nesteds(pathopvals)
      |> default_partition_filter(opts)
      |> EctoQueryBQ.SQL.to_sql()

    with {:ok, queryresult} <- do_query(project_id, sql, params) do
      %{schema: schema, rows: rows} = queryresult
      rows = SchemaUtils.merge_rows_with_schema(schema, rows)
      rows = rows || []
      {:ok, %SearchResult{rows: rows}}
    else
      err ->
        {:error, %{body: body}} = err
        body = Jason.decode!(body)
        IO.warn(hd(body["error"]["errors"])["message"])
        err
    end
  end

  def do_query(project_id, sql, params) do
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

  def default_partition_filter(q, %SearchOpts{partitions: nil}), do: q

  def default_partition_filter(q, %SearchOpts{partitions: {start_date, end_date}} = opts) do
    if Timex.equal?(end_date, Date.utc_today()) do
      q
      |> where([log], fragment("_PARTITIONTIME BETWEEN ? and ?", ^start_date, ^end_date))
      |> or_where([log], fragment("_PARTITIONTIME IS NULL"))
    else
      filter_partitions(q, opts)
    end
  end

  def filter_by_streaming_filter(q, _), do: where(q, [l], fragment("_PARTITIONTIME IS NULL"))

  def filter_partitions(q, %SearchOpts{partitions: nil}), do: q

  def filter_partitions(q, %SearchOpts{partitions: {start_date, from_date}}) do
    where(
      q,
      [log],
      fragment("_PARTITIONTIME BETWEEN ? and ?", ^start_date, ^from_date)
    )
  end
end
