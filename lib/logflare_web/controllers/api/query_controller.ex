defmodule LogflareWeb.Api.QueryController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  alias Logflare.Alerting
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.VictoriaMetricsAdaptor
  alias Logflare.Backends.Backend
  alias Logflare.Endpoints
  alias Logflare.Endpoints.EndpointQuery
  alias Logflare.SingleTenant
  alias Logflare.Sql
  alias Logflare.User
  alias LogflareWeb.OpenApi.BadRequest
  alias LogflareWeb.OpenApi.One
  alias LogflareWeb.OpenApi.Unauthorized
  alias LogflareWeb.OpenApiSchemas.PromQLQueryResponse
  alias LogflareWeb.OpenApiSchemas.QueryParseResult
  alias LogflareWeb.OpenApiSchemas.QueryResult
  alias OpenApiSpex.Schema

  action_fallback(LogflareWeb.Api.FallbackController)

  tags(["management"])

  operation(:parse,
    summary: "Parses a query",
    parameters: [
      sql: [
        in: :query,
        description:
          "SQL string. Preferred parameter; backend_id can select backend-specific SQL language for parsing.",
        type: :string,
        allowEmptyValue: true,
        required: false
      ],
      bq_sql: [
        in: :query,
        description: "Deprecated BigQuery SQL parameter. Prefer sql with backend_id.",
        type: :string,
        required: false,
        example: "select current_timestamp() as 'test'"
      ],
      ch_sql: [
        in: :query,
        description: "Deprecated ClickHouse SQL parameter. Prefer sql with backend_id.",
        type: :string,
        required: false,
        example: "select now() as 'test'"
      ],
      pg_sql: [
        in: :query,
        description: "Deprecated PostgreSQL SQL parameter. Prefer sql with backend_id.",
        type: :string,
        required: false,
        example: "select current_date() as 'test'"
      ],
      backend_id: [
        in: :query,
        description:
          "Optional backend ID used to infer SQL language for parsing. If omitted, BigQuery SQL is used.",
        type: :integer,
        required: false
      ]
    ],
    responses: %{
      200 => One.response(QueryParseResult),
      400 => BadRequest.response(),
      401 => Unauthorized.response()
    }
  )

  def parse(%{assigns: %{user: user}} = conn, params) do
    endpoints = Endpoints.list_endpoints_by(user_id: user.id)

    alerts = Alerting.list_alert_queries_by_user_id(user.id)

    with {:ok, requested_language, sql} <- extract_query(params),
         {:ok, backend} <- fetch_backend(user, params),
         language = resolve_language(requested_language, backend),
         {:ok, result} <- Endpoints.parse_query_string(language, sql, endpoints, alerts),
         {:ok, _transformed_query} <- Sql.transform(language, sql, user.id) do
      json(conn, %{result: result})
    end
  end

  operation(:query,
    summary: "Execute a query",
    parameters: [
      promql: [
        in: :query,
        description:
          "Raw PromQL expression. Requires a VictoriaMetrics backend_id and cannot be combined with SQL parameters.",
        type: :string,
        required: false,
        example: "sum(rate(http_requests_total[5m]))"
      ],
      sql: [
        in: :query,
        description:
          "SQL string. Preferred parameter; backend_id selects the backend to execute against and its SQL language.",
        type: :string,
        required: false
      ],
      bq_sql: [
        in: :query,
        description: "Deprecated BigQuery SQL parameter. Prefer sql with backend_id.",
        type: :string,
        required: false,
        example: "select current_timestamp() as 'test'"
      ],
      ch_sql: [
        in: :query,
        description: "Deprecated ClickHouse SQL parameter. Prefer sql with backend_id.",
        type: :string,
        required: false,
        example: "select now() as 'test'"
      ],
      pg_sql: [
        in: :query,
        description: "Deprecated PostgreSQL SQL parameter. Prefer sql with backend_id.",
        type: :string,
        required: false,
        example: "select current_date() as 'test'"
      ],
      backend_id: [
        in: :query,
        description:
          "Backend ID to execute the query against. Required for PromQL; determines the language for SQL.",
        type: :integer,
        required: false
      ],
      time: [
        in: :query,
        description:
          "PromQL instant query evaluation time as a Unix timestamp or RFC3339 string.",
        type: :string,
        required: false
      ],
      start: [
        in: :query,
        description:
          "PromQL range start as a Unix timestamp or RFC3339 string. Requires end and step.",
        type: :string,
        required: false
      ],
      end: [
        in: :query,
        description:
          "PromQL range end as a Unix timestamp or RFC3339 string. Requires start and step.",
        type: :string,
        required: false
      ],
      step: [
        in: :query,
        description:
          "PromQL range resolution as seconds or a duration. Cannot be combined with time.",
        type: :string,
        required: false
      ],
      timeout: [
        in: :query,
        description: "PromQL query evaluation timeout as a duration.",
        type: :string,
        required: false
      ]
    ],
    responses: %{
      200 =>
        {"SQL rows or a native Prometheus response", "application/json",
         %Schema{oneOf: [QueryResult, PromQLQueryResponse]}},
      400 =>
        {"Invalid query", "application/json",
         %Schema{anyOf: [BadRequest.schema(), PromQLQueryResponse]}},
      401 =>
        {"Unauthorized", "application/json",
         %Schema{anyOf: [Unauthorized.schema(), PromQLQueryResponse]}},
      "4XX" => {"PromQL request error", "application/json", PromQLQueryResponse},
      "5XX" => {"PromQL backend error", "application/json", PromQLQueryResponse},
      :default => {"PromQL error", "application/json", PromQLQueryResponse}
    }
  )

  def query(%{assigns: %{user: user}} = conn, %{"promql" => query} = params) do
    with :ok <- validate_promql_params(params),
         {:ok, backend} <- fetch_backend(user, params, :promql),
         {:ok, response} <-
           VictoriaMetricsAdaptor.execute_promql(
             backend,
             query,
             Map.take(params, ~w(time start end step timeout))
           ) do
      json(conn, response)
    else
      {:error, {status, response}} ->
        conn |> put_status(status) |> json(response)

      {:error, message} ->
        conn
        |> put_status(400)
        |> json(%{status: "error", errorType: "bad_data", error: message})
    end
  end

  def query(%{assigns: %{user: user}} = conn, params) do
    with {:ok, requested_language, sql} <- extract_query(params),
         {:ok, backend} <- fetch_backend(user, params),
         language = resolve_language(requested_language, backend),
         opts = build_query_opts(backend),
         {:ok, %{rows: rows}} <- Endpoints.run_query_string(user, {language, sql}, opts) do
      json(conn, %{result: rows})
    end
  end

  @spec validate_promql_params(map()) :: :ok | {:error, String.t()}
  defp validate_promql_params(params) do
    if Enum.any?(~w(sql bq_sql ch_sql pg_sql), &Map.has_key?(params, &1)) do
      {:error, "promql cannot be combined with SQL parameters"}
    else
      :ok
    end
  end

  @spec extract_query(map()) ::
          {:ok, :infer | :bq_sql | :ch_sql | :pg_sql, String.t()} | {:error, String.t()}
  defp extract_query(%{"sql" => sql}), do: {:ok, :infer, sql}
  defp extract_query(%{"bq_sql" => sql}), do: {:ok, :bq_sql, sql}
  defp extract_query(%{"ch_sql" => sql}), do: {:ok, :ch_sql, sql}
  defp extract_query(%{"pg_sql" => sql}), do: {:ok, :pg_sql, sql}

  defp extract_query(_) do
    {:error,
     "No query params provided. Supported query params are sql=, bq_sql=, ch_sql=, and pg_sql="}
  end

  @spec resolve_language(:infer | :bq_sql | :ch_sql | :pg_sql, Backend.t() | nil) ::
          :bq_sql | :ch_sql | :pg_sql
  defp resolve_language(:infer, backend),
    do: EndpointQuery.map_backend_to_language(backend, SingleTenant.supabase_mode?())

  defp resolve_language(language, _backend), do: language

  @spec fetch_backend(User.t(), map(), :sql | :promql) ::
          {:ok, Backend.t() | nil} | {:error, String.t()}
  defp fetch_backend(user, params, query_type \\ :sql)

  defp fetch_backend(_user, %{"backend_id" => backend_id}, :sql) when backend_id in [nil, ""],
    do: {:ok, nil}

  defp fetch_backend(_user, %{"backend_id" => backend_id}, :promql) when backend_id in [nil, ""],
    do: {:error, "backend_id is required for PromQL queries"}

  defp fetch_backend(user, %{"backend_id" => backend_id}, query_type)
       when is_binary(backend_id) do
    case Integer.parse(backend_id) do
      {id, ""} -> fetch_backend(user, %{"backend_id" => id}, query_type)
      _ -> {:error, "Invalid backend_id: must be an integer"}
    end
  end

  defp fetch_backend(_user, %{"backend_id" => backend_id}, :promql)
       when is_integer(backend_id) and backend_id not in 1..9_223_372_036_854_775_807,
       do: {:error, "Invalid backend_id: must be a positive 64-bit integer"}

  defp fetch_backend(user, %{"backend_id" => backend_id}, query_type)
       when is_integer(backend_id) do
    case Backends.get_backend(backend_id) do
      %Backend{user_id: user_id} = backend when user_id == user.id ->
        validate_backend_type(backend, query_type)

      _ ->
        {:error, "Backend not found"}
    end
  end

  defp fetch_backend(_user, %{"backend_id" => _backend_id}, :promql),
    do: {:error, "Invalid backend_id: must be an integer"}

  defp fetch_backend(_user, _params, :promql),
    do: {:error, "backend_id is required for PromQL queries"}

  defp fetch_backend(_user, _params, :sql), do: {:ok, nil}

  @spec validate_backend_type(Backend.t(), :sql | :promql) ::
          {:ok, Backend.t()} | {:error, String.t()}
  defp validate_backend_type(%Backend{type: :victoria_metrics} = backend, :promql),
    do: {:ok, backend}

  defp validate_backend_type(_backend, :promql),
    do: {:error, "Backend does not support PromQL queries"}

  defp validate_backend_type(backend, :sql) do
    if Backends.Adaptor.can_query?(backend) do
      {:ok, backend}
    else
      {:error, "Backend does not support querying"}
    end
  end

  @spec build_query_opts(Backend.t() | nil) :: keyword()
  defp build_query_opts(nil), do: []
  defp build_query_opts(%Backend{id: id}), do: [backend_id: id]
end
