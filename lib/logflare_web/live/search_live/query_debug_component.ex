defmodule LogflareWeb.Search.QueryDebugComponent do
  @moduledoc """
  LiveView Component to render components
  """
  use Phoenix.LiveComponent
  alias LogflareWeb.SearchView

  def render(assigns) do
    search_op =
      case assigns.id do
        :modal_debug_error_link -> assigns.search_op_error
        :modal_debug_log_events_link -> assigns.search_op_log_events
        :modal_debug_log_aggregates_link -> assigns.search_op_log_aggregates
      end

    SearchView.render("debug.html", search_op: search_op, user: assigns.user)
  end

  def update(assigns, socket) do
    socket = assign(socket, assigns)

    {:ok, socket}
  end

  def mount(socket) do
    {:ok, socket}
  end
end
