defmodule LogflareWeb.SourceView do
  import LogflareWeb.Helpers.Forms
  alias LogflareWeb.Router.Helpers, as: Routes
  use LogflareWeb, :view

  def log_url(route) do
    url = Routes.log_url(LogflareWeb.Endpoint, route) |> URI.parse()

    case url do
      %URI{authority: "logflare.app"} ->
        Map.put(url, :authority, "api.logflare.app")

      _ ->
        url
    end
    |> URI.to_string()
  end
end
