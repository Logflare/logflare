defmodule LogflareWeb.EndpointsController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  require Logger
  alias Logflare.Endpoints

  alias LogflareWeb.JsonParser
  alias LogflareWeb.OpenApi.Unauthorized
  alias LogflareWeb.OpenApi.ServerError
  alias LogflareWeb.OpenApiSchemas.EndpointQuery

  @plug_parsers_init Plug.Parsers.init(
                       parsers: [JsonParser],
                       json_decoder: Jason,
                       body_reader: {PlugCaisson, :read_body, []}
                     )

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
      "X-API-Key",
      "LF-ENDPOINT-LABELS"
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
      401 => Unauthorized.response(),
      500 => ServerError.response()
    }
  )

  plug(:parse_get_body)

  def query(%{assigns: %{endpoint: endpoint}} = conn, params) do
    endpoint_query = Endpoints.map_query_sources(endpoint)

    header_str =
      get_req_header(conn, "lf-endpoint-labels")
      |> case do
        [str] -> str
        _ -> ""
      end

    parsed_labels = Endpoints.parse_labels(endpoint_query.labels, header_str, params)

    case Endpoints.run_cached_query(%{endpoint_query | parsed_labels: parsed_labels}, params) do
      {:ok, result} ->
        Logger.debug("Endpoint cache result, #{inspect(result, pretty: true)}")
        render(conn, "query.json", result: result.rows)

      {:error, errors} ->
        render(conn, "query.json", errors: errors)
    end
  end

  # only parse body for get when ?sql= is empty and it is sandboxable
  # passthrough for all other cases
  defp parse_get_body(
         %{method: "GET", assigns: %{endpoint: %_{sandboxable: true}}, query_params: qp} = conn,
         _opts
       )
       when is_map_key(qp, "sql") == false do
    conn
    # Plug.Parsers only supports POST/PUT/PATCH
    |> Map.put(:method, "POST")
    |> Map.put(:body_params, %Plug.Conn.Unfetched{})
    |> Plug.Parsers.call(@plug_parsers_init)
    |> Map.put(:method, "GET")
  end

  defp parse_get_body(conn, _opts), do: conn
end
