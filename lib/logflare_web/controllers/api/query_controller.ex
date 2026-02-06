defmodule LogflareWeb.Api.QueryController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  alias Logflare.Alerting
  alias Logflare.Backends
  alias Logflare.Backends.Backend
  alias Logflare.Endpoints
  alias Logflare.Endpoints.Query
  alias Logflare.Sql
  alias Logflare.User
  alias LogflareWeb.OpenApi.BadRequest
  alias LogflareWeb.OpenApi.List
  alias LogflareWeb.OpenApi.One
  alias LogflareWeb.OpenApiSchemas.QueryParseResult
  alias LogflareWeb.OpenApiSchemas.QueryResult

  action_fallback(LogflareWeb.Api.FallbackController)

  tags(["management"])

  operation(:parse,
    summary: "Parses a query",
    parameters: [
      sql: [
        in: :query,
        description: "BigQuery SQL string, alias for bq_sql",
        type: :string,
        allowEmptyValue: true,
        required: false
      ],
      bq_sql: [
        in: :query,
        description: "BigQuery SQL string",
        type: :string,
        required: false,
        example: "select current_timestamp() as 'test'"
      ],
      ch_sql: [
        in: :query,
        description: "ClickHouse SQL string",
        type: :string,
        required: false,
        example: "select now() as 'test'"
      ]
    ],
    responses: %{
      200 => One.response(QueryParseResult),
      400 => BadRequest.response()
    }
  )

  def parse(conn, %{"sql" => sql}), do: parse(conn, %{"bq_sql" => sql})

  def parse(%{assigns: %{user: user}} = conn, %{"bq_sql" => sql}) do
    endpoints = Endpoints.list_endpoints_by(user_id: user.id)

    alerts = Alerting.list_alert_queries_by_user_id(user.id)

    with {:ok, result} <- Endpoints.parse_query_string(:bq_sql, sql, endpoints, alerts),
         {:ok, _transformed_query} <- Sql.transform(:bq_sql, sql, user.id) do
      json(conn, %{result: result})
    end
  end

  def parse(%{assigns: %{user: user}} = conn, %{"ch_sql" => sql}) do
    endpoints = Endpoints.list_endpoints_by(user_id: user.id)

    alerts = Alerting.list_alert_queries_by_user_id(user.id)

    with {:ok, result} <- Endpoints.parse_query_string(:ch_sql, sql, endpoints, alerts),
         {:ok, _transformed_query} <- Sql.transform(:ch_sql, sql, user.id) do
      json(conn, %{result: result})
    end
  end

  operation(:query,
    summary: "Execute a query",
    parameters: [
      sql: [
        in: :query,
        description: "SQL string",
        type: :string,
        required: false
      ],
      bq_sql: [
        in: :query,
        description: "BigQuery SQL string",
        type: :string,
        required: false,
        example: "select current_timestamp() as 'test'"
      ],
      ch_sql: [
        in: :query,
        description: "ClickHouse SQL string",
        type: :string,
        required: false,
        example: "select now() as 'test'"
      ],
      pg_sql: [
        in: :query,
        description: "PostgresSQL string",
        type: :string,
        required: false,
        example: "select current_date() as 'test'"
      ],
      backend_id: [
        in: :query,
        description:
          "Backend ID to execute the query against. When provided with sql=, the language is inferred from the backend type.",
        type: :integer,
        required: false
      ]
    ],
    responses: %{200 => List.response(QueryResult)}
  )

  def query(%{assigns: %{user: user}} = conn, params) do
    with {:ok, language, sql} <- extract_query(params),
         {:ok, backend} <- fetch_backend(user, params),
         language = resolve_language(language, backend),
         opts = build_query_opts(backend),
         {:ok, %{rows: rows}} <- Endpoints.run_query_string(user, {language, sql}, opts) do
      json(conn, %{result: rows})
    end
  end

  @spec extract_query(map()) :: {:ok, atom(), String.t()} | {:error, String.t()}
  defp extract_query(%{"sql" => sql}), do: {:ok, :infer, sql}
  defp extract_query(%{"bq_sql" => sql}), do: {:ok, :bq_sql, sql}
  defp extract_query(%{"ch_sql" => sql}), do: {:ok, :ch_sql, sql}
  defp extract_query(%{"pg_sql" => sql}), do: {:ok, :pg_sql, sql}

  defp extract_query(_) do
    {:error,
     "No query params provided. Supported query params are sql=, bq_sql=, ch_sql=, and pg_sql="}
  end

  @spec resolve_language(atom(), Backend.t() | nil) :: atom()
  defp resolve_language(:infer, backend), do: Query.map_backend_to_language(backend, false)
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
