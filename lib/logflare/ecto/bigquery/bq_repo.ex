defmodule Logflare.BqRepo do
  @moduledoc false
  alias GoogleApi.BigQuery.V2.Api
  alias GoogleApi.BigQuery.V2.Model.QueryRequest
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.EctoQueryBQ
  alias Logflare.Billing
  alias Logflare.Billing.Plan
  alias Logflare.User
  import Logflare.TypeCasts
  require Logger

  @query_request_timeout 25_000
  @use_query_cache true
  @type results :: %{
          :rows => nil | [term()],
          :num_rows => non_neg_integer(),
          optional(atom()) => any()
        }
  @type query_result ::
          {:ok, results()} | {:error, term()}

  @spec query_with_sql_and_params(
          Logflare.User.t(),
          String.t(),
          String.t(),
          maybe_improper_list,
          maybe_improper_list
        ) :: query_result()
  def query_with_sql_and_params(%User{} = user, project_id, sql, params, opts \\ [])
      when not is_nil(project_id) and is_binary(sql) and is_list(params) and is_list(opts) do
    override = Map.new(opts)
    %Plan{name: plan} = Billing.Cache.get_plan_by_user(user)

    # Clean labels using format_key for both key and value
    cleaned_labels =
      %{
        "managed_by" => "logflare",
        "logflare_plan" => plan,
        "logflare_account" => user.id
      }
      |> Map.merge(Map.get(override, :labels, %{}))
      |> Enum.map(fn {k, v} ->
        {GenUtils.format_key(k), GenUtils.format_key(v)}
      end)
      |> Enum.into(%{})

    override = Map.put(override, :labels, cleaned_labels)

    query_request =
      %{
        query: sql,
        useLegacySql: false,
        useQueryCache: @use_query_cache,
        parameterMode: "POSITIONAL",
        queryParameters: params,
        jobCreationMode: "JOB_CREATION_OPTIONAL",
        dryRun: false,
        jobTimeoutMs: @query_request_timeout,
        timeoutMs: @query_request_timeout,
        labels: cleaned_labels
      }
      |> DeepMerge.deep_merge(override)
      |> then(fn map -> struct(QueryRequest, map) end)

    result =
      GenUtils.get_conn({:query, user})
      |> Api.Jobs.bigquery_jobs_query(project_id, body: query_request)
      |> GenUtils.maybe_parse_google_api_result()
      |> warn_if_cost_above_limit(query_request.labels, user)

    with {:ok, response} <- result do
      response =
        response
        |> Map.update!(:rows, &SchemaUtils.merge_rows_with_schema(response.schema, &1))
        |> Map.update(:totalBytesProcessed, 0, &maybe_string_to_integer_or_zero/1)
        |> Map.update(:totalRows, 0, &maybe_string_to_integer_or_zero/1)
        |> Map.from_struct()
        |> Enum.map(fn {key, value} ->
          key =
            key
            |> Atom.to_string()
            |> Recase.to_snake()
            |> String.to_atom()

          {key, value}
        end)
        |> Map.new()

      {:ok, response}
    end
  end

  @spec query(Logflare.User.t(), String.t(), Ecto.Query.t(), maybe_improper_list) ::
          query_result()
  def query(%User{} = user, project_id, %Ecto.Query{} = query, opts \\ [])
      when not is_nil(project_id) and is_list(opts) do
    {sql, params} = EctoQueryBQ.SQL.to_sql_params(query)

    sql =
      if opts[:dataset_id] do
        EctoQueryBQ.SQL.substitute_dataset(sql, opts[:dataset_id])
      else
        sql
      end

    query_with_sql_and_params(user, project_id, sql, params, opts)
  end

  defp warn_if_cost_above_limit(
         {:ok, %{totalBytesProcessed: total_bytes_processed}} = result,
         request_labels,
         user
       )
       when is_binary(total_bytes_processed) do
    %{bigquery_processed_bytes_limit: limit} = user

    labels =
      Enum.reduce(request_labels, [], fn {key, value}, acc ->
        acc ++ ["#{key}: #{value}"]
      end)
      |> Enum.join(", ")

    if String.to_integer(total_bytes_processed) > user.bigquery_processed_bytes_limit do
      Logger.warning(
        "Query bytes exceeded plan. limit: #{limit}, bytes: #{total_bytes_processed}, #{labels}",
        user: user.id
      )
    end

    result
  end

  defp warn_if_cost_above_limit(result, _request_labels, _user), do: result
end
