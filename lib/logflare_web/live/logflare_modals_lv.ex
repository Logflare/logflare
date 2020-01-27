defmodule LogflareWeb.ModalHelpersLV do
  @moduledoc """
  Modal helpers to be imported where modals may be called
  """
  import LogflareWeb.SearchLV.Utils
  import Phoenix.LiveView, only: [assign: 3]

  defmacro __using__(_context) do
    quote do
      def handle_event("activate_modal" = ev, metadata, socket) do
        log_lv_received_event(ev, socket.assigns.source)
        modal_id = metadata["modal_id"]

        {:noreply, assign(socket, :active_modal, modal_id)}
      end

      def handle_event("deactivate_modal" = ev, metadata, socket) do
        if metadata["key"] == "Escape" or is_nil(metadata["code"]) do
          log_lv_received_event(ev, socket.assigns.source)

          {:noreply, assign(socket, :active_modal, nil)}
        else
          {:noreply, socket}
        end
      end
    end
  end
end
