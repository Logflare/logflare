defmodule LogflareWeb.SourceView do
  import LogflareWeb.Helpers.Forms
  alias LogflareWeb.Router.Helpers, as: Routes
  alias Logflare.Billing.Plan
  alias Logflare.Source
  alias Logflare.Google.BigQuery.GenUtils
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
  def source_ttl_to_days(%Source{bigquery_table_ttl: ttl}, _plan)
      when ttl >= 0 and ttl != nil do
    round(ttl)
  end

  # fallback to plan value or default init value
  # use min to avoid misrepresenting what user should see, in cases where actual is more than plan.
  def source_ttl_to_days(_source, %Plan{limit_source_ttl: ttl}) do
    min(
      round(GenUtils.default_table_ttl_days()),
      round(ttl / :timer.hours(24))
    )
  end
end
