defmodule Logflare.BqRepo do
  @moduledoc false
  alias GoogleApi.BigQuery.V2.Api
  alias GoogleApi.BigQuery.V2.Model.Job
  alias GoogleApi.BigQuery.V2.Model.JobConfiguration
  alias GoogleApi.BigQuery.V2.Model.JobConfigurationQuery
  alias GoogleApi.BigQuery.V2.Model.QueryRequest
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.Billing
  alias Logflare.Billing.Plan
  alias Logflare.User
  import Logflare.TypeCasts
  require Logger

  @query_request_timeout 25_000
  @batch_poll_interval 500
  @use_query_cache true
  @type results :: %{
          :rows => nil | [term()],
          :num_rows => non_neg_integer(),
          optional(atom()) => any()
        }
  @type query_result ::
          {:ok, results()} | {:error, any()}

  @spec query_with_sql_and_params(
          Logflare.User.t(),
          String.t(),
          String.t(),
          maybe_improper_list,
          maybe_improper_list
        ) :: query_result()
  def query_with_sql_and_params(%User{} = user, project_id, sql, params, opts \\ [])
      when not is_nil(project_id) and is_binary(sql) and is_list(params) and is_list(opts) do
    %Plan{name: plan} = Billing.Cache.get_plan_by_user(user)

    override = Map.new(opts)
    override_labels = Map.get(override, :labels, %{}) |> Map.to_list()

    cleaned_labels =
      [
        {"managed_by", "logflare"},
        {"logflare_plan", plan},
        {"logflare_account", user.id}
        | override_labels
      ]
      |> Map.new(fn {k, v} ->
        {GenUtils.format_key(k), GenUtils.format_value(v)}
      end)

    override = Map.put(override, :labels, cleaned_labels)
    use_query_cache = Keyword.get(opts, :use_query_cache, @use_query_cache)

    query_request =
      %{
        query: sql,
        useLegacySql: false,
        useQueryCache: use_query_cache,
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
      |> execute_query_request(project_id, query_request, opts)
      |> warn_if_cost_above_limit(query_request.labels, user)

    with {:ok, response} <- result do
      {:ok, transform_response(response)}
    end
  end

  defp execute_query_request(conn, project_id, query_request, opts) do
    if batch_query?(opts) do
      execute_batch_query(conn, project_id, query_request)
    else
      conn
      |> Api.Jobs.bigquery_jobs_query(project_id, body: query_request)
      |> GenUtils.maybe_parse_google_api_result()
    end
  end

  defp execute_batch_query(conn, project_id, %QueryRequest{} = query_request) do
    job = build_batch_query_job(query_request)

    with {:ok, %Job{jobReference: job_reference}} <-
           Api.Jobs.bigquery_jobs_insert(conn, project_id, body: job) do
      poll_batch_query_results(
        conn,
        project_id,
        job_reference,
        query_request,
        batch_poll_deadline()
      )
    else
      {:ok, %Job{} = job} ->
        {:error, job}

      result ->
        GenUtils.maybe_parse_google_api_result(result)
    end
  end

  defp build_batch_query_job(%QueryRequest{} = query_request) do
    query_config =
      query_request
      |> Map.from_struct()
      |> Map.put(:priority, "BATCH")
      |> then(&struct(JobConfigurationQuery, &1))

    configuration =
      %JobConfiguration{
        dryRun: query_request.dryRun,
        jobTimeoutMs: query_request.jobTimeoutMs,
        labels: query_request.labels,
        query: query_config,
        reservation: query_request.reservation
      }

    %Job{configuration: configuration}
  end

  defp poll_batch_query_results(_conn, _project_id, nil, _query_request, _deadline) do
    {:error, :missing_job_reference}
  end

  defp poll_batch_query_results(_conn, _project_id, %{jobId: nil}, _query_request, _deadline) do
    {:error, :missing_job_id}
  end

  defp poll_batch_query_results(conn, project_id, job_reference, query_request, deadline) do
    job_id = job_reference.jobId
    location = job_reference.location || query_request.location

    opts =
      [
        location: location,
        maxResults: query_request.maxResults,
        timeoutMs: @query_request_timeout
      ]
      |> Enum.reject(fn {_key, value} -> is_nil(value) end)

    case Api.Jobs.bigquery_jobs_get_query_results(conn, project_id, job_id, opts) do
      {:ok, %{jobComplete: true, errors: [_ | _] = errors}} ->
        {:error, errors}

      {:ok, %{jobComplete: true} = response} ->
        {:ok, response}

      {:ok, %{jobComplete: false}} ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:error, :timeout}
        else
          Process.sleep(@batch_poll_interval)
          poll_batch_query_results(conn, project_id, job_reference, query_request, deadline)
        end

      result ->
        GenUtils.maybe_parse_google_api_result(result)
    end
  end

  defp batch_poll_deadline do
    System.monotonic_time(:millisecond) + @query_request_timeout
  end

  defp batch_query?(opts) do
    Keyword.get(opts, :job_priority) in [:batch, "BATCH"]
  end

  defp transform_response(response) do
    response
    |> Map.update!(:rows, &SchemaUtils.merge_rows_with_schema(response.schema, &1))
    |> Map.update(:totalBytesProcessed, 0, &maybe_string_to_integer_or_zero/1)
    |> Map.update(:totalRows, 0, &maybe_string_to_integer_or_zero/1)
    |> Map.from_struct()
    |> Map.new(fn {key, value} ->
      snake_key = key |> Atom.to_string() |> Recase.to_snake() |> String.to_atom()
      {snake_key, value}
    end)
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
