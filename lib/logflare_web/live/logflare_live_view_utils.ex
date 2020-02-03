defmodule LogflareWeb.LiveViewUtils do
  @moduledoc """
  Utilites for LiveViews
  """
  import Phoenix.LiveView, only: [assign: 2]

  defmacro __using__(_context) do
    quote do
      import LogflareWeb.LiveViewUtils, only: [assign_flash: 3]

      def handle_info({:lvc_assigns, key, value}, socket) do
        socket = assign(socket, key, value)
        {:noreply, socket}
      end

      def handle_event("remove_flash" = ev, metadata, socket) do
        key = metadata["flash_key"]

        socket =
          if key do
            key = String.to_existing_atom(key)
            socket = assign_flash(socket, key, nil)
          else
            socket
          end

        {:noreply, socket}
      end
    end
  end

  def assign_flash(%{assigns: %{flash: flash}} = socket, key, message) do
    assign(socket, flash: put_in(flash, [key], message))
  end
end
