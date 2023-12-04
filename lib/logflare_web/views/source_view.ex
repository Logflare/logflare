defmodule LogflareWeb.SourceView do
  import LogflareWeb.Helpers.Forms
  alias LogflareWeb.Router.Helpers, as: Routes
  alias Logflare.Billing.Plan
  alias Logflare.Source
  use LogflareWeb, :view

  def log_url(route) do
    url = Routes.log_url(LogflareWeb.Endpoint, route) |> URI.parse()

    case url do
      %URI{authority: "logflare.app"} ->
        url
        |> Map.put(:authority, "api.logflare.app")
        |> Map.put(:host, "api.logflare.app")

      _ ->
        url
    end
    |> URI.to_string()
  end

  @doc """
  Formats a source TTL to the specified unit
  """
  @spec source_ttl_to_days(Source.t(), Plan.t()) :: integer()
  def source_ttl_to_days(%Source{bigquery_table_ttl: nil} = source, %Plan{} = plan) do
    source_ttl_to_days(%{source | bigquery_table_ttl: plan.limit_source_ttl}, :day)
  end

  def source_ttl_to_days(%Source{bigquery_table_ttl: ttl}, _plan) do
    round(ttl / :timer.hours(24))
  end
end
