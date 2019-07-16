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

  defmodule SearchOperation do
    @moduledoc """
    Logs search options and result
    """
    use TypedStruct

    typedstruct do
      field :source, Source.t()
      field :querystring, String.t()
      field :query, Ecto.Query.t()
      field :query_result, term()
      field :sql_params, {term(), term()}
      field :tailing?, boolean
      field :tailing_initial?, boolean
      field :rows, [map()]
      field :pathvalops, [map()]
      field :error, term()
      field :stats, :map
    end
  end

  alias SearchOperation, as: SO

  def search(so) do
    so
    |> Map.put(:stats, %{total_duration: System.monotonic_time(:millisecond)})
    |> default_from
    |> parse_querystring()
    |> partition_or_streaming()
    |> apply_wheres()
    |> apply_to_sql()
    |> do_query()
    |> process_query_result()
    |> put_stats()
    |> case do
      %{error: nil} = so ->
        {:ok, so}

      so ->
        # body = Jason.decode!(body)
        # IO.warn(hd(body["error"]["errors"])["message"])
        # err
        {:error, so}
    end
  end

  def do_query(so) do
    %SO{source: %Source{token: source_id}} = so
    project_id = GenUtils.get_project_id(source_id)
    conn = GenUtils.get_conn()

    {sql, params} = so.sql_params

    conn
    |> Api.Jobs.bigquery_jobs_query(
      project_id,
      body: %QueryRequest{
        query: sql,
        useLegacySql: false,
        useQueryCache: true,
        parameterMode: "POSITIONAL",
        queryParameters: params
      }
    )
    |> put_result_in(so, :query_result)
    |> prepare_query_result()
  end

  def prepare_query_result(so) do
    %{so | query_result:  so.query_result
    |> Map.update!(:totalBytesProcessed, &String.to_integer/1)
    |> Map.update!(:totalRows, &String.to_integer/1)
    |> AtomicMap.convert(%{safe: false})}
  end

  def put_stats(so) do
    stats = so.stats
    |> Map.merge(%{
      total_rows: so.query_result.total_rows,
      total_bytes_processed: so.query_result.total_bytes_processed
    })
    |> Map.update!(:total_duration, & System.monotonic_time(:millisecond) - &1)

    %{so | stats: stats}
   end

  def process_query_result(so) do
    %{schema: schema, rows: rows} = so.query_result
    rows = SchemaUtils.merge_rows_with_schema(schema, rows)
    %{so | rows: rows}
  end

  def default_from(so) do
    %{so | query: from(so.source.bq_table_id, select: [:timestamp, :event_message, :metadata])}
  end

  def apply_to_sql(so) do
    %{so | sql_params: EctoQueryBQ.SQL.to_sql(so.query)}
  end

  def apply_wheres(so) do
    %{so | query: EctoQueryBQ.where_nesteds(so.query, so.pathvalops)}
  end

  def parse_querystring(so) do
    so.querystring
    |> Parser.parse()
    |> put_result_in(so, :pathvalops)
  end

  def put_result_in({:ok, value}, so, path) when is_atom(path), do: %{so | path => value}
  def put_result_in({:error, term}, so, _), do: %{so | error: term}

  def partition_or_streaming(%SO{tailing?: true, tailing_initial?: true} = so) do
    query =
      where(
        so.query,
        [log],
        fragment(
          "TIMESTAMP_ADD(_PARTITIONTIME, INTERVAL 24 HOUR) > CURRENT_TIMESTAMP() OR _PARTITIONTIME IS NULL"
        )
      )

    so
    |> Map.put(:query, query)
    |> drop_timestamp_pathvalops
  end

  def partition_or_streaming(%SO{tailing?: true} = so) do
    so
    |> Map.update!(:query, &query_only_streaming_buffer/1)
    |> drop_timestamp_pathvalops
  end

  def partition_or_streaming(so), do: so

  def drop_timestamp_pathvalops(so) do
    %{so | pathvalops: Enum.reject(so.pathvalops, &(&1.path === "timestamp"))}
  end

  def query_only_streaming_buffer(q), do: where(q, [l], fragment("_PARTITIONTIME IS NULL"))
end
