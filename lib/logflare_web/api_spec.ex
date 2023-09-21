defmodule LogflareWeb.ApiSpec do
  alias LogflareWeb.Endpoint
  alias LogflareWeb.Router

  alias OpenApiSpex.Components
  alias OpenApiSpex.Info
  alias OpenApiSpex.OpenApi
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
      paths: Paths.from_router(Router) |> filter_routes(),
      components: %Components{
        securitySchemes: %{
          "authorization" => %SecurityScheme{type: "http", scheme: "bearer", bearerFormat: "JWT"}
        }
      }
    })
  end

  defp filter_routes(routes_map) do
    for {"/api" <> _path = k, v} <- routes_map, into: %{} do
      {k, v}
    end
  end
end
