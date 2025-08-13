defmodule LogflareWeb.Api.QueryController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  alias Logflare.Endpoints
  alias LogflareWeb.OpenApi.List
  alias LogflareWeb.OpenApi.BadRequest
  alias LogflareWeb.OpenApi.One
  alias LogflareWeb.OpenApiSchemas.QueryParseResult
  alias LogflareWeb.OpenApiSchemas.QueryResult
  alias Logflare.Alerting
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
         {:ok, _transformed_query} <- Logflare.Sql.transform(:bq_sql, sql, user.id) do
      json(conn, %{result: result})
    end
  end

  def parse(%{assigns: %{user: user}} = conn, %{"ch_sql" => sql}) do
    endpoints = Endpoints.list_endpoints_by(user_id: user.id)

    alerts = Alerting.list_alert_queries_by_user_id(user.id)

    with {:ok, result} <- Endpoints.parse_query_string(:ch_sql, sql, endpoints, alerts),
         {:ok, _transformed_query} <- Logflare.Sql.transform(:ch_sql, sql, user.id) do
      json(conn, %{result: result})
    end
  end

  operation(:query,
    summary: "Execute a query",
    parameters: [
      sql: [
        in: :query,
        description: "BigQuery SQL string, alias for bq_sql",
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
      ]
    ],
    responses: %{200 => List.response(QueryResult)}
  )

  def query(conn, %{"sql" => sql}), do: query(conn, %{"bq_sql" => sql})

  def query(%{assigns: %{user: user}} = conn, %{"ch_sql" => sql}) do
    with {:ok, %{rows: rows}} <- Endpoints.run_query_string(user, {:ch_sql, sql}) do
      json(conn, %{result: rows})
    end
  end

  def query(%{assigns: %{user: user}} = conn, %{"pg_sql" => sql}) do
    with {:ok, %{rows: rows}} <- Endpoints.run_query_string(user, {:pg_sql, sql}) do
      json(conn, %{result: rows})
    end
  end

  def query(%{assigns: %{user: user}} = conn, %{"bq_sql" => sql}) do
    with {:ok, %{rows: rows}} <- Endpoints.run_query_string(user, {:bq_sql, sql}) do
      json(conn, %{result: rows})
    end
  end

  def query(_conn, _params) do
    {:error,
     "No query params provided. Supported query params are sql=, bq_sql=, ch_sql=, and pg_sql="}
  end
end
