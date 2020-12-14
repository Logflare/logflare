defmodule LogflareWeb.Source.SearchLV.DebugLVC do
  @moduledoc false
  use Phoenix.LiveComponent
  alias LogflareWeb.SearchView

  def render(assigns) do
    SearchView.render("debug.html", assigns)
  end

  def update(assigns, socket) do
    socket = assign(socket, assigns)

    {:ok, socket}
  end

  def mount(socket) do
    {:ok, socket}
  end
end
