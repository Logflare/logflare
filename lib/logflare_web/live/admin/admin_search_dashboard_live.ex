defmodule LogflareWeb.AdminSearchDashboardLive do
  use Phoenix.LiveView
  alias LogflareWeb.AdminView
  alias Logflare.SavedSearches.Analytics
  alias Logflare.SavedSearches
  alias Logflare.Rules
  use LogflareWeb.LiveViewUtils

  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(:top_filters_paths, Analytics.top_field_paths(:lql_filters))
      |> assign(:top_charts_paths, Analytics.top_field_paths(:lql_charts))
      |> assign(:search_timeseries, Analytics.search_timeseries())
      |> assign(:saved_searches, Analytics.saved_searches())
      |> assign(:operators_filters, Analytics.operators())
      |> assign(:source_timeseries, Analytics.source_timeseries())
      |> assign(:user_timeseries, Analytics.user_timeseries())
      |> assign(:top_sources, Analytics.top_sources(:"24h"))

    {:ok, socket}
  end

  def render(assigns) do
    AdminView.render("search_dashboard.html", assigns)
  end

  def handle_event("upgrade_rules_lql_filters", _metadata, socket) do
    Rules.upgrade_all_source_rules_to_next_lql_version()

    socket =
      assign_notifications(
        socket,
        :warning,
        "Source rules upgrade to latest LQL filters is in process.."
      )

    {:noreply, socket}
  end

  def handle_event("upgrade_saved_searches", _metadata, socket) do
    SavedSearches.mark_as_saved_by_users()
    SavedSearches.update_lql_rules_where_nil()
    socket = assign_notifications(socket, :warning, "Saved search update is in process..")
    {:noreply, socket}
  end
end
