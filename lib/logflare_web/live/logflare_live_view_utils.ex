defmodule LogflareWeb.LiveViewUtils do
  @moduledoc """
  Utilites for LiveViews
  """
  import Phoenix.LiveView, only: [assign: 2]

  defmacro __using__(_context) do
    quote do
      import LogflareWeb.LiveViewUtils, only: [assign_notifications: 3]

      # def handle_info({:lvc_assigns, key, value}, socket) do
      #   socket = assign(socket, key, value)
      #   {:noreply, socket}
      # end

      def handle_event("remove_notifications" = ev, metadata, socket) do
        key = metadata["notifications_key"]

        socket =
          if key do
            key = String.to_existing_atom(key)
            socket = assign_notifications(socket, key, nil)
          else
            socket
          end

        {:noreply, socket}
      end
    end
  end

  def assign_notifications(%{assigns: %{notifications: notifications}} = socket, key, message) do
    assign(socket, notifications: put_in(notifications, [key], message))
  end

  def assign_notifications(socket, key, message) do
    notifications = %{}
    assign(socket, notifications: put_in(notifications, [key], message))
  end
end
