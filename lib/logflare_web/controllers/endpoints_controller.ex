defmodule LogflareWeb.EndpointsController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  require Logger

  alias Logflare.Backends.QueryError
  alias Logflare.Endpoints
  alias LogflareWeb.Api.FallbackController
  alias LogflareWeb.JsonParser
  alias LogflareWeb.OpenApi.BadRequest
  alias LogflareWeb.OpenApi.NotFound
  alias LogflareWeb.OpenApi.ServerError
  alias LogflareWeb.OpenApi.Unauthorized
  alias LogflareWeb.OpenApiSchemas.EndpointQuery
  alias LogflareWeb.QueryErrorHelpers

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
      "LF-ENDPOINT-LABELS",
      "LF-ENDPOINT-REDACT-PII",
      "LF-ENDPOINT-BIGQUERY-RESERVATION",
      "LF-ENDPOINT-VERSION"
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
      ],
      lf_endpoint_version: [
        in: :header,
        name: "LF-ENDPOINT-VERSION",
        description: "Endpoint version number to execute",
        type: :integer,
        required: false
      ]
    ],
    responses: %{
      200 => EndpointQuery.response(),
      400 => BadRequest.response(),
      401 => Unauthorized.response(),
      404 => NotFound.response(),
      500 => ServerError.response()
    }
  )

  plug(:load_endpoint_query)
  plug(:parse_get_body)

  def query(%{assigns: %{endpoint_query: endpoint_query}} = conn, params) do
    header_str =
      get_req_header(conn, "lf-endpoint-labels")
      |> case do
        [str] -> str
        _ -> ""
      end

    parsed_labels = Endpoints.parse_labels(endpoint_query.labels, header_str, params)
    override = get_req_header(conn, "lf-endpoint-redact-pii") |> List.first()

    redact_pii =
      if override, do: String.downcase(override) == "true", else: endpoint_query.redact_pii

    reservation =
      if endpoint_query.enable_dynamic_reservation do
        get_req_header(conn, "lf-endpoint-bigquery-reservation") |> List.first()
      end

    case Endpoints.run_cached_query(
           %{endpoint_query | parsed_labels: parsed_labels},
           params,
           redact_pii: redact_pii,
           reservation: reservation
         ) do
      {:ok, result} ->
        Logger.debug("Endpoint cache result, #{inspect(result, pretty: true)}")
        render(conn, "query.json", result: result.rows)

      {:error, error = %QueryError{}} ->
        render(conn, "query.json", error: QueryErrorHelpers.query_error_message(error))

      {:error, _errors} ->
        render(conn, "query.json", error: QueryErrorHelpers.generic_query_error_message())
    end
  end

  @spec load_endpoint_query(Plug.Conn.t(), any()) :: Plug.Conn.t()
  defp load_endpoint_query(%{assigns: %{endpoint: endpoint}} = conn, _opts) do
    with [version_str | _] <- get_req_header(conn, "lf-endpoint-version"),
         {:ok, version_number} <- parse_endpoint_version(version_str),
         {:ok, endpoint_query} <-
           Endpoints.get_endpoint_query_at_version(endpoint, version_number) do
      assign(conn, :endpoint_query, endpoint_query)
    else
      [] ->
        assign(conn, :endpoint_query, Endpoints.map_query_sources(endpoint))

      {:error, :version_not_found} ->
        conn
        |> FallbackController.call({:error, :not_found, "version not found"})
        |> halt()

      _ ->
        conn
        |> FallbackController.call({:error, "invalid lf-endpoint-version"})
        |> halt()
    end
  end

  # only parse body for get when `?sql=` and `?lql=` are empty
  # passthrough for all other cases
  defp parse_get_body(
         %{method: "GET", assigns: %{endpoint_query: %{sandboxable: true}}, query_params: qp} =
           conn,
         _opts
       )
       when not is_map_key(qp, "sql") and not is_map_key(qp, "lql") do
    parse_get_body(conn)
  end

  defp parse_get_body(conn, _opts), do: conn

  @spec parse_get_body(Plug.Conn.t()) :: Plug.Conn.t()
  defp parse_get_body(conn) do
    conn
    # Plug.Parsers only supports POST/PUT/PATCH
    |> Map.put(:method, "POST")
    |> Map.put(:body_params, %Plug.Conn.Unfetched{})
    |> Plug.Parsers.call(@plug_parsers_init)
    |> Map.put(:method, "GET")
  end

  @spec parse_endpoint_version(String.t()) :: {:ok, pos_integer()} | {:error, :invalid_version}
  defp parse_endpoint_version(version_str) do
    case Integer.parse(version_str) do
      {version_number, ""} when version_number > 0 ->
        {:ok, version_number}

      _ ->
        {:error, :invalid_version}
    end
  end
end
