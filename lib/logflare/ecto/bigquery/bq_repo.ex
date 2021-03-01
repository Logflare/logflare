defmodule Logflare.BqRepo do
  @moduledoc false
  alias GoogleApi.BigQuery.V2.Api
  alias GoogleApi.BigQuery.V2.Model.QueryRequest
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.EctoQueryBQ
  alias Logflare.TypeCasts

  @query_request_timeout 60_000
  @use_query_cache true
  @type query_result ::
          {:ok,
           %{:rows => nil | [term()], :num_rows => non_neg_integer(), optional(atom()) => any()}}
          | {:error, term()}

  @spec query_with_sql_and_params(String.t(), String.t(), [term()], Keyword.t()) :: query_result
  def query_with_sql_and_params(project_id, sql, params, opts \\ [])
      when not is_nil(project_id) and is_binary(sql) and is_list(params) and is_list(opts) do
    override = Map.new(opts)

    query_request =
      %QueryRequest{
        query: sql,
        useLegacySql: false,
        useQueryCache: @use_query_cache,
        parameterMode: "POSITIONAL",
        queryParameters: params,
        dryRun: false,
        timeoutMs: @query_request_timeout
      }
      |> Map.merge(override)

    result =
      GenUtils.get_conn()
      |> Api.Jobs.bigquery_jobs_query(
        project_id,
        body: query_request
      )
      |> GenUtils.maybe_parse_google_api_result()

    with {:ok, response} <- result do
      response =
        response
        |> Map.update!(:rows, &SchemaUtils.merge_rows_with_schema(response.schema, &1))
        |> Map.update(:totalBytesProcessed, 0, &Typecasts.maybe_string_to_integer_or_zero/1)
        |> Map.update(:totalRows, 0, &Typecasts.maybe_string_to_integer_or_zero/1)
        |> Map.from_struct()
        |> Enum.map(fn {k, v} ->
          {
            k |> Atom.to_string() |> Recase.to_snake() |> String.to_atom(),
            v
          }
        end)
        |> Map.new()
        |> MapKeys.to_atoms_unsafe!()

      {:ok, response}
    else
      errtup -> errtup
    end
  end

  @spec query(String.t(), Ecto.Query.t(), keyword) :: query_result
  def query(project_id, %Ecto.Query{} = query, opts \\ [])
      when not is_nil(project_id) and is_list(opts) do
    {sql, params} = EctoQueryBQ.SQL.to_sql_params(query)

    sql =
      if opts[:dataset_id] do
        EctoQueryBQ.SQL.substitute_dataset(sql, opts[:dataset_id])
      else
        sql
      end

    query_with_sql_and_params(project_id, sql, params, opts)
  end
end
