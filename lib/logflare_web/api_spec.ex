defmodule LogflareWeb.ApiSpec do
  alias OpenApiSpex.Info
  alias OpenApiSpex.OpenApi
  alias OpenApiSpex.Paths
  alias OpenApiSpex.Server

  alias LogflareWeb.Endpoint
  alias LogflareWeb.Router

  @behaviour OpenApi

  @impl OpenApi
  def spec do
    OpenApiSpex.resolve_schema_modules(%OpenApi{
      servers: [Server.from_endpoint(Endpoint)],
      info: %Info{
        title: to_string(Application.spec(:logflare, :description)),
        version: to_string(Application.spec(:logflare, :vsn))
      },
      paths: Paths.from_router(Router)
    })
  end
end
