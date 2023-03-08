defmodule LogflareWeb.EndpointsController do
  use LogflareWeb, :controller
  require Logger
  alias Logflare.Endpoints

  action_fallback(LogflareWeb.Api.FallbackController)

  plug CORSPlug,
    origin: "*",
    max_age: 1_728_000,
    headers: [
      "Authorization",
      "Content-Type",
      "Content-Length",
      "X-Requested-With",
      "X-API-Key"
    ],
    methods: ["GET", "POST", "OPTIONS"],
    send_preflight_response?: true

  def query(%{assigns: %{endpoint: endpoint}} = conn, _params) do
    endpoint_query = Endpoints.map_query_sources(endpoint)

    with {:ok, result} <- Endpoints.run_cached_query(endpoint_query, conn.query_params) do
      Logger.debug("Endpoint cache result, #{inspect(result, pretty: true)}")
      render(conn, "query.json", result: result.rows)
    else
      {:error, err} ->
        render(conn, "query.json", error: err)
    end
  end
end
