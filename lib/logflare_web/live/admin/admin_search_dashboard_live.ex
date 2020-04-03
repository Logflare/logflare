defmodule LogflareWeb.AdminSearchDashboardLive do
  use Phoenix.LiveView
  alias LogflareWeb.AdminView
  alias Logflare.SavedSearches.Analytics
  alias Logflare.SavedSearches

  def mount(_params, _session, socket) do
    # remove after first run
    SavedSearches.mark_as_saved_by_users()

    socket =
      socket
      |> assign(:top_filters_paths, Analytics.top_field_paths(:lql_filters))
      |> assign(:top_charts_paths, Analytics.top_field_paths(:lql_charts))
      |> assign(:search_timeseries, Analytics.search_timeseries())
      |> assign(:saved_searches, Analytics.saved_searches())
      |> assign(:operators_filters, Analytics.operators())
      |> assign(:source_timeseries, Analytics.source_timeseries())
      |> assign(:user_timeseries, Analytics.user_timeseries())

    {:ok, socket}
  end

  def render(assigns) do
    AdminView.render("search_dashboard.html", assigns)
  end
end
