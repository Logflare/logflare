defmodule LogflareWeb.Api.QueryController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  alias Logflare.Endpoints
  alias LogflareWeb.OpenApi.List
  alias LogflareWeb.OpenApiSchemas.QueryResult

  action_fallback(LogflareWeb.Api.FallbackController)

  tags(["management"])

  operation(:query,
    summary: "Execute a query",
    responses: %{200 => List.response(QueryResult)}
  )

  def query(conn, %{"sql" => sql}), do: query(conn, %{"bq_sql" => sql})

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
    {:error, "No query params provided. Supported query params are sql=, bq_sql=, and pg_sql="}
  end
end
