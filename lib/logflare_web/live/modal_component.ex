defmodule LogflareWeb.ModalComponent do
  use LogflareWeb, :live_component

  @impl true
  def render(assigns) do
    ~L"""
    <div id="logflare-modal" phx-hook="LiveModal">
      <div id="<%= @id %>" class="modal fade show"
          phx-capture-click="close"
          phx-window-keydown="close"
          phx-key="escape"
          phx-target="#<%= @id %>"
          phx-page-loading
          style="display: block;"
          >
        <div class="modal-dialog modal-xl" role="document">
          <div class="modal-content">
            <div class="modal-header">
              <h5 class="modal-title"><%= @title %> </h5>
              <%= live_patch raw("&times;"), to: @return_to, class: "phx-modal-close" %>
            </div>
            <div class="modal-body">
              <div class="container">
                <%= live_component @socket, @component, @opts %>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
    """
  end

  @impl true
  def handle_event("close", _, socket) do
    socket =
      socket
      |> push_patch(to: socket.assigns.return_to)

    {:noreply, socket}
  end
end
