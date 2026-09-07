defmodule LogflareWeb.Backends.Components do
  use Phoenix.Component

  # TODO: Extract common components (e.g. inputs) from backend_form

  attr :status, :atom, values: [:ok, :error, :loading]

  def status_indicator(assigns) do
    ~H"""
    <.async_result :let={_ok} assign={@status}>
      <:loading><.indicator icon="spinner" color="tw-text-white" animation="tw-animate-spin" )} /></:loading>
      <:failed :let={reason}>
        <.indicator icon="times" color="tw-text-red-500" )} />
        <span class="inline-block tw-mx-15">
          {status_error_message(reason)}
        </span>
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

  defp status_error_message({:error, %Logflare.Backends.QueryError{} = query_error}) do
    LogflareWeb.QueryErrorHelpers.query_error_message(query_error)
  end

  defp status_error_message(_reason) do
    LogflareWeb.QueryErrorHelpers.generic_query_error_message()
  end
end
