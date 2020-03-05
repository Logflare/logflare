defmodule Logflare.BqRepo do
  @moduledoc false
  alias GoogleApi.BigQuery.V2.Api
  alias GoogleApi.BigQuery.V2.Model.QueryRequest
  alias Logflare.Google.BigQuery.GenUtils

  @query_request_timeout 60_000
  @use_query_cache true

  @spec query(String.t(), String.t(), [term()], Keyword.t()) ::
          {:ok,
           %{:rows => nil | [term()], :num_rows => non_neg_integer(), optional(atom()) => any()}}
          | {:error, term()}
  def query(project_id, sql, params, opts \\ [])
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
      AtomicMap.convert(response, %{safe: false})
    else
      errtup -> errtup
    end
  end
end
