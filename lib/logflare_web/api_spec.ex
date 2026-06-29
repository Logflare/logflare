defmodule LogflareWeb.ApiSpec do
  alias LogflareWeb.Endpoint
  alias LogflareWeb.Router

  alias LogflareWeb.OpenApi.Unauthorized

  alias OpenApiSpex.Components
  alias OpenApiSpex.Info
  alias OpenApiSpex.OpenApi
  alias OpenApiSpex.Operation
  alias OpenApiSpex.Paths
  alias OpenApiSpex.SecurityScheme
  alias OpenApiSpex.Server

  @behaviour OpenApi

  @impl OpenApi
  def spec do
    OpenApiSpex.resolve_schema_modules(%OpenApi{
      servers: [Server.from_endpoint(Endpoint)],
      info: %Info{
        title: to_string(Application.spec(:logflare, :description)),
        version: to_string(Application.spec(:logflare, :vsn))
      },
      paths:
        Router
        |> Paths.from_router()
        |> add_management_auth_responses()
        |> filter_routes(),
      components: %Components{
        securitySchemes: %{
          "authorization" => %SecurityScheme{type: "http", scheme: "bearer", bearerFormat: "JWT"}
        }
      }
    })
  end

  @spec management_route_operations() :: [{String.t(), atom()}]
  def management_route_operations do
    Router.__routes__()
    |> Enum.filter(&management_route?/1)
    |> Enum.map(&{open_api_path(&1.path), &1.verb})
  end

  defp add_management_auth_responses(paths) do
    Enum.reduce(management_route_operations(), paths, fn {path, verb}, paths ->
      case Map.get(paths, path) do
        path_item when not is_nil(path_item) ->
          case Map.get(path_item, verb) do
            %Operation{} = operation ->
              operation = %{
                operation
                | responses: Map.put_new(operation.responses, 401, unauthorized_response())
              }

              Map.put(paths, path, Map.put(path_item, verb, operation))

            nil ->
              paths
          end

        nil ->
          paths
      end
    end)
  end

  defp unauthorized_response do
    Operation.response("Unauthorized", "application/json", Unauthorized)
  end

  defp management_route?(route) do
    case route_info(route) do
      %{pipe_through: pipe_through} -> :require_mgmt_api_auth in pipe_through
      :error -> false
    end
  end

  defp route_info(route) do
    Phoenix.Router.route_info(
      Router,
      route.verb |> Atom.to_string() |> String.upcase(),
      String.replace(route.path, ~r|:[^/]+|, "route-param"),
      "localhost"
    )
  end

  defp open_api_path(path) do
    Regex.replace(~r|:([^/]+)|, path, fn _, parameter -> "{#{parameter}}" end)
  end

  defp filter_routes(routes_map) do
    for {"/api" <> _path = k, v} <- routes_map, into: %{} do
      {k, v}
    end
  end
end
