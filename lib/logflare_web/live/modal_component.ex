defmodule LogflareWeb.ModalComponent do
  @moduledoc false
  use LogflareWeb, :live_component

  @doc """
  Renders a modal component.

  # Customize close behavior

  An optional JS command can assigned to `close` to trigger an event when the user closed the modal.
  You must append `JS.push("close")` to the command to close the modal, which is the default if `close` is not provided.

  ```
  <%= modal(close: JS.push("do_something") |> JS.push("close"), ...) %>
  ```

  """
  @impl true
  def render(assigns) do
    assigns =
      assigns
      |> assign(:close, assigns[:close] || "close")

    ~H"""
    <div id={@id} class="modal fade show" phx-hook="LiveModal" phx-click-away={@close} phx-window-keydown={@close} phx-key="escape" phx-target={"##{@id}"} style="display: block;">
      <div class="modal-dialog modal-xl" role="document">
        <div class="modal-content">
          <div class="modal-header lf-modal-header">
            <h5 class="modal-title">{@title}</h5>
            <span>
              {link(raw("&times;"),
                to: "#",
                class: "phx-modal-close",
                phx_click: @close,
                phx_target: "##{@id}"
              )}
            </span>
          </div>
          <div class="modal-body">
            <div class="container">
              <%= if @is_template? do %>
                {render(@view, @template, assigns)}
              <% else %>
                <%= if assigns[:live_view] do %>
                  {live_render(@socket, @live_view, @opts)}
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
