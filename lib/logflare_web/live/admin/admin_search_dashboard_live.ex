defmodule LogflareWeb.AdminSearchDashboardLive do
  use Phoenix.LiveView
  alias LogflareWeb.AdminView
  alias Logflare.SavedSearches.Analytics

  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(:top_filters_paths, Analytics.top_field_paths(:lql_filters))
      |> assign(:top_charts_paths, Analytics.top_field_paths(:lql_charts))
      |> assign(:search_timeseries, Analytics.search_timeseries())
      |> assign(:saved_searches, Analytics.saved_searches())

    {:ok, socket}
  end

  def render(assigns) do
    AdminView.render("search_dashboard.html", assigns)
  end
end
