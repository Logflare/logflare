defmodule LogflareWeb.ModalComponent do
  @moduledoc false
  use LogflareWeb, :live_component

  @impl true
  def render(assigns) do
    ~H"""
    <div id={@id} class="modal fade show" phx-hook="LiveModal" phx-capture-click="close" phx-window-keydown="close" phx-key="escape" phx-target={"##{@id}"} phx-page-loading style="display: block;">
      <div class="modal-dialog modal-xl" role="document">
        <div class="modal-content">
          <div class="modal-header lf-modal-header">
            <h5 class="modal-title"><%= @title %></h5>
            <span>
              <%= link(raw("&times;"),
                to: "#",
                class: "phx-modal-close",
                phx_click: "close",
                phx_target: "##{@id}"
              ) %>
            </span>
          </div>
          <div class="modal-body">
            <div class="container">
              <%= if @is_template? do %>
                <%= render(@view, @template, assigns) %>
              <% else %>
                <%= if assigns[:live_view] do %>
                  <%= live_render(@socket, @live_view, @opts) %>
                <% else %>
                  <.live_component module={@component} {@opts} />
                <% end %>
              <% end %>
            </div>
          </div>
        </div>
      </div>
    </div>
    """
  end

  @impl true
  def handle_event("close", _, %{assigns: %{return_to: return_to}} = socket)
      when is_binary(return_to) do
    socket =
      socket
      |> push_patch(to: socket.assigns.return_to)

    {:noreply, socket}
  end

  @impl true
  def handle_event("close", _, %{assigns: %{return_to: rt}} = socket)
      when is_nil(rt)
      when rt == false do
    send(self(), :hide_modal)
    {:noreply, socket}
  end
end
