defmodule LogflareWeb.AlertComponent do
  @moduledoc false
  use Phoenix.LiveComponent
  attr(:alert_class, :string, required: true)
  attr(:key, :string, required: true)
  attr(:id, :string, required: true)
  slot(:inner_block)

  def render(assigns) do
    ~H"""
    <div class="message">
      <div class={"inner-message alert alert-#{@alert_class} tw-min-w-[350px]"} role="alert">
        <p>{render_slot(@inner_block)}</p>
        <a href="#" phx-click="close" phx-target={@myself} phx-value-flash_key={@key}>
          <button type="button" class="close">
            &times;
          </button>
        </a>
      </div>
    </div>
    """
  end

  def handle_event("close", %{"flash_key" => key}, socket) do
    send(self(), {:clear_flash, String.to_atom(key)})

    {:noreply, socket}
  end
end
