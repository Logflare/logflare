defmodule LogflareWeb.EndpointsController do
  use LogflareWeb, :controller
  require Logger
  alias Logflare.Endpoints

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

  def query(conn, %{"token" => token}) do
    endpoint_query = Endpoints.get_mapped_query_by_token(token)

    case Endpoints.run_cached_query(endpoint_query, conn.query_params) do
      {:ok, result} ->
        Logger.debug("Endpoint cache result, #{inspect(result, pretty: true)}")
        render(conn, "query.json", result: result.rows)

      {:error, err} ->
        render(conn, "query.json", error: err)
    end
  end
end
