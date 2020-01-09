defmodule LogflareWeb.Source.SearchLV.DebugLVC do
  use Phoenix.LiveComponent
  alias LogflareWeb.SearchView
  alias Logflare.Lql
  alias Logflare.Sources
  alias LogflareWeb.LqlView
  import LogflareWeb.LiveComponentUtils

  def render(assigns) do
    Phoenix.View.render(SearchView, "debug.html", assigns)
  end

  def update(assigns, socket) do
    socket =
      socket
      |> assign(assigns)

    {:ok, socket}
  end

  def mount(socket) do
    {:ok, socket}
  end
end
