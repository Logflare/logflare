defmodule LogflareWeb.AlertComponent do
  use LogflareWeb, :live_component

  @impl true
  def render(assigns) do
    ~L"""
      <div class="message">
        <div class="alert alert-<%= @alert_class %> alert-live" role="alert">
          <span><%= @value %></span>
          <a href="#" phx-click="close"
                phx-target="<%= @myself %>"
                phx-value-flash_key="<%= @key %>"
                >
            <button type="button" class="close" >
              &times;
            </button>
          </a>
        </div>
      </div>
    """
  end

  @impl true
  def handle_event("close", %{"flash_key" => key}, socket) do
    send(self(), {:clear_flash, String.to_atom(key)})

    {:noreply, socket}
  end
end
