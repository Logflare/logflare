defmodule LogflareWeb.LiveViewUtils do
  @moduledoc """
  Utilites for LiveViews
  """

  defmacro __using__(_context) do
    quote do
      def handle_info({:lvc_assigns, key, value}, socket) do
        socket = assign(socket, key, value)
        {:noreply, socket}
      end
    end
  end
end
