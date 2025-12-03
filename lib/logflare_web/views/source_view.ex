defmodule LogflareWeb.SourceView do
  import LogflareWeb.Helpers.Forms
  alias LogflareWeb.Router.Helpers, as: Routes
  alias LogflareWeb.Utils
  use LogflareWeb, :view

  def log_url(route) do
    url = Routes.log_url(LogflareWeb.Endpoint, route) |> URI.parse()

    case url do
      %URI{host: "logflare.app"} ->
        url
        |> Map.put(:authority, "api.logflare.app")
        |> Map.put(:host, "api.logflare.app")

      _ ->
        url
    end
    |> URI.to_string()
  end
end
