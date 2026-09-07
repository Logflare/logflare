defmodule LogflareWeb.SourceView do
  use LogflareWeb, :view

  import LogflareWeb.Helpers.Forms

  alias Logflare.Billing.Plan
  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias LogflareWeb.Helpers.BqSchema
  alias LogflareWeb.Router.Helpers, as: Routes
  alias LogflareWeb.Utils

  @spec default_source_retention_days(Plan.t()) :: integer()
  def default_source_retention_days(plan) do
    Sources.source_ttl_to_days(%Source{}, plan)
  end

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
