defmodule Logflare.Logs.Search do
  @moduledoc false
  alias Logflare.Google.BigQuery.{GenUtils, SchemaUtils}
  alias Logflare.{Source, Sources, EctoQueryBQ}
  alias Logflare.Logs.Search.Parser
  import Ecto.Query

  alias GoogleApi.BigQuery.V2.Api
  alias GoogleApi.BigQuery.V2.Model.QueryRequest

  use Logflare.GenDecorators

  @default_limit 100

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

  def search(%SO{} = so) do
    so
    |> Map.put(:stats, %{
      start_monotonic_time: System.monotonic_time(:millisecond),
      total_duration: nil
    })
    |> default_from
    |> parse_querystring()
    |> verify_path_in_schema()
    |> partition_or_streaming()
    |> apply_wheres()
    |> order_by_default()
    |> apply_limit_to_query()
    |> apply_to_sql()
    |> do_query()
    |> process_query_result()
    |> put_stats()
    |> case do
      %{error: nil} = so ->
        {:ok, so}

      so ->
        {:error, so}
    end
  end

  @decorate pass_through_on_error_field()
  def do_query(%SO{} = so) do
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

  @decorate pass_through_on_error_field()
  def prepare_query_result(%SO{} = so) do
    %{
      so
      | query_result:
          so.query_result
          |> Map.update!(:totalBytesProcessed, &String.to_integer/1)
          |> Map.update!(:totalRows, &String.to_integer/1)
          |> AtomicMap.convert(%{safe: false})
    }
  end

  @decorate pass_through_on_error_field()
  def order_by_default(%SO{} = so) do
    %{so | query: order_by(so.query, desc: :timestamp)}
  end

  @decorate pass_through_on_error_field()
  def apply_limit_to_query(%SO{} = so) do
    %{so | query: limit(so.query, @default_limit)}
  end

  @decorate pass_through_on_error_field()
  def put_stats(%SO{} = so) do
    stats =
      so.stats
      |> Map.merge(%{
        total_rows: so.query_result.total_rows,
        total_bytes_processed: so.query_result.total_bytes_processed
      })
      |> Map.put(
        :total_duration,
        System.monotonic_time(:millisecond) - so.stats.start_monotonic_time
      )

    %{so | stats: stats}
  end

  @decorate pass_through_on_error_field()
  def process_query_result(%SO{} = so) do
    %{schema: schema, rows: rows} = so.query_result
    rows = SchemaUtils.merge_rows_with_schema(schema, rows)
    %{so | rows: rows}
  end

  @decorate pass_through_on_error_field()
  def default_from(%SO{} = so) do
    %{so | query: from(so.source.bq_table_id, select: [:timestamp, :event_message, :metadata])}
  end

  @decorate pass_through_on_error_field()
  def apply_to_sql(%SO{} = so) do
    %{so | sql_params: EctoQueryBQ.SQL.to_sql(so.query)}
  end

  @decorate pass_through_on_error_field()
  def apply_wheres(%SO{} = so) do
    %{so | query: EctoQueryBQ.where_nesteds(so.query, so.pathvalops)}
  end

  @decorate pass_through_on_error_field()
  def parse_querystring(%SO{} = so) do
    so.querystring
    |> Parser.parse()
    |> put_result_in(so, :pathvalops)
  end

  def put_result_in(_, so, path \\ nil)
  def put_result_in(:ok, so, _), do: so
  def put_result_in({:ok, value}, so, path) when is_atom(path), do: %{so | path => value}
  def put_result_in({:error, term}, so, _), do: %{so | error: term}

  @decorate pass_through_on_error_field()
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

  @decorate pass_through_on_error_field()
  def partition_or_streaming(%SO{tailing?: true} = so) do
    so
    |> Map.update!(:query, &query_only_streaming_buffer/1)
    |> drop_timestamp_pathvalops
  end

  def partition_or_streaming(%SO{} = so), do: so

  @decorate pass_through_on_error_field()
  def drop_timestamp_pathvalops(%SO{} = so) do
    %{so | pathvalops: Enum.reject(so.pathvalops, &(&1.path === "timestamp"))}
  end

  def query_only_streaming_buffer(q), do: where(q, [l], fragment("_PARTITIONTIME IS NULL"))

  @decorate pass_through_on_error_field()
  def verify_path_in_schema(%SO{} = so) do
    flatmap =
      so.source
      |> Sources.Cache.get_bq_schema()
      |> Logflare.Logs.Validators.BigQuerySchemaChange.to_typemap()
      |> Iteraptor.to_flatmap()
      |> Enum.map(fn {k, v} -> {String.replace(k, ".fields", ""), v} end)
      |> Enum.map(fn {k, _} -> String.trim_trailing(k, ".t") end)

    result =
      Enum.reduce_while(so.pathvalops, :ok, fn %{path: path}, _ ->
        if path in flatmap do
          {:cont, :ok}
        else
          {:halt, {:error, "#{path} not present in source schema"}}
        end
      end)

    put_result_in(result, so)
  end
end
