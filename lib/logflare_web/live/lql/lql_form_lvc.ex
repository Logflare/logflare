defmodule LogflareWeb.Lql.LqlFormLVC do
  use Phoenix.LiveComponent
  alias Logflare.Lql
  alias Logflare.Sources
  alias LogflareWeb.LqlView
  import LogflareWeb.LiveComponentUtils

  def render(assigns) do
    LqlView.render("lql_form.html", assigns)
  end

  def update(assigns, socket) do
    socket =
      socket
      |> assign(assigns)
      |> assign(:loading, false)

    {:ok, socket}
  end

  def mount(socket) do
    {:ok, socket}
  end
end
