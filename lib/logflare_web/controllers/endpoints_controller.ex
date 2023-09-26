defmodule LogflareWeb.EndpointsController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  require Logger
  alias Logflare.Endpoints

  alias LogflareWeb.OpenApi.ServerError
  alias LogflareWeb.OpenApiSchemas.EndpointQuery

  action_fallback(LogflareWeb.Api.FallbackController)
  tags(["Public"])

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

  operation(:query,
    summary: "Query a Logflare Endpoint",
    description:
      "Full details are available in the [Logflare Endpoints documentation](https://docs.logflare.app/concepts/endpoints/)",
    parameters: [
      token_or_name: [
        in: :path,
        description: "Endpoint UUID or name",
        type: :string,
        example: "a040ae88-3e27-448b-9ee6-622278b23193",
        required: true
      ]
    ],
    responses: %{
      200 => EndpointQuery.response(),
      500 => ServerError.response()
    }
  )

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
