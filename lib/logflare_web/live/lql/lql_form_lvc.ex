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
