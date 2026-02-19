defmodule LogflareWeb.Backends.Components do
  use Phoenix.Component

  # TODO: Extract common components (e.g. inputs) from backend_form

  attr :status, :atom, values: [:ok, :error, :loading]

  def status_indicator(assigns) do
    ~H"""
    <.async_result :let={_ok} assign={@status}>
      <:loading><.indicator icon="spinner" color="tw-text-white" animation="tw-animate-spin" )} /></:loading>
      <:failed :let={{atom, reason}}>
        <% reason = if atom == :error, do: reason, else: "Internal error" %>
        <.indicator icon="times" color="tw-text-red-500" )} /> <span class="inline-block tw-mx-15">{reason}</span>
      </:failed>
      <.indicator icon="check" color="tw-text-green-500" )} />
    </.async_result>
    """
  end

  defp indicator(assigns) do
    animation = if assigns[:animation], do: assigns.animation, else: ""
    assigns = assign(assigns, animation: animation)

    ~H"""
    <i class={"inline-block fas fa-#{@icon} #{@color} tw-align-middle tw-text-2xl #{@animation}"}></i>
    """
  end
end
