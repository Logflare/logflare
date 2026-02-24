defmodule LogflareWeb.Lql.LqlFormLVC do
  @moduledoc """
  LiveView component for LQL form
  """
  use LogflareWeb, :live_component

  alias LogflareWeb.LqlHelpers

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
