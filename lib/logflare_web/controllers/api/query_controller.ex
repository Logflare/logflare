defmodule LogflareWeb.Api.QueryController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  alias Logflare.Alerting
  alias Logflare.Backends
  alias Logflare.Backends.Backend
  alias Logflare.Endpoints
  alias Logflare.Endpoints.EndpointQuery
  alias Logflare.SingleTenant
  alias Logflare.Sql
  alias Logflare.User
  alias LogflareWeb.OpenApi.BadRequest
  alias LogflareWeb.OpenApi.One
  alias LogflareWeb.OpenApi.Unauthorized
  alias LogflareWeb.OpenApiSchemas.QueryParseResult
  alias LogflareWeb.OpenApiSchemas.QueryResult

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
          "Backend ID to execute the query against. The backend type determines the SQL language.",
        type: :integer,
        required: false
      ]
    ],
    responses: %{
      200 => One.response(QueryResult),
      400 => BadRequest.response(),
      401 => Unauthorized.response()
    }
  )

  def query(%{assigns: %{user: user}} = conn, params) do
    with {:ok, requested_language, sql} <- extract_query(params),
         {:ok, backend} <- fetch_backend(user, params),
         language = resolve_language(requested_language, backend),
         opts = build_query_opts(backend),
         {:ok, %{rows: rows}} <- Endpoints.run_query_string(user, {language, sql}, opts) do
      json(conn, %{result: rows})
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

  @spec fetch_backend(User.t(), map()) :: {:ok, Backend.t() | nil} | {:error, String.t()}
  defp fetch_backend(_user, %{"backend_id" => backend_id}) when backend_id in [nil, ""],
    do: {:ok, nil}

  defp fetch_backend(user, %{"backend_id" => backend_id}) when is_binary(backend_id) do
    case Integer.parse(backend_id) do
      {id, ""} -> fetch_backend(user, %{"backend_id" => id})
      _ -> {:error, "Invalid backend_id: must be an integer"}
    end
  end

  defp fetch_backend(user, %{"backend_id" => backend_id}) when is_integer(backend_id) do
    case Backends.get_backend(backend_id) do
      %Backend{user_id: user_id} = backend when user_id == user.id ->
        if Backends.Adaptor.can_query?(backend) do
          {:ok, backend}
        else
          {:error, "Backend does not support querying"}
        end

      _ ->
        {:error, "Backend not found"}
    end
  end

  defp fetch_backend(_user, _params), do: {:ok, nil}

  @spec build_query_opts(Backend.t() | nil) :: keyword()
  defp build_query_opts(nil), do: []
  defp build_query_opts(%Backend{id: id}), do: [backend_id: id]
end
