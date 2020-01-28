defmodule LogflareWeb.Lql.LqlFormLVC do
  @moduledoc """
  LiveView component for LQL form
  """
  use Phoenix.LiveComponent
  alias LogflareWeb.LqlView

  def render(assigns) do
    LqlView.render("lql_form.html", assigns)
  end

  def update(assigns, socket) do
    socket = assign(socket, assigns)

    {:ok, socket}
  end

  def mount(socket) do
    socket =
      socket
      |> assign(:loading, false)

    {:ok, socket}
  end
end
