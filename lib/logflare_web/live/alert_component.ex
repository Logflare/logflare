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
    case existing_flash_key(key) do
      {:ok, flash_key} -> send(self(), {:clear_flash, flash_key})
      :error -> :ok
    end

    {:noreply, socket}
  end

  # Flash keys are atoms set by our own code. Never create new atoms from
  # client-supplied event params, atoms are not garbage collected.
  defp existing_flash_key(key) do
    {:ok, String.to_existing_atom(key)}
  rescue
    ArgumentError -> :error
  end
end
