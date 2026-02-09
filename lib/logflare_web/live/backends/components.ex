defmodule LogflareWeb.Backends.Components do
  use Phoenix.Component

  attr :status, :atom, values: [:ok, :error, :loading]

  def status_indicator(assigns) do
    case assigns.status do
      :ok -> ~H|<.indicator icon="check" color="green-500" )} />|
      :error -> ~H|<.indicator icon="times" color="red-500" )} />|
      :loading -> ~H|<.indicator icon="spinner" color="white" animation="tw-animate-spin" )} />|
      nil -> ~H""
    end
  end

  defp indicator(assigns) do
    animation = if assigns[:animation], do: assigns.animation, else: ""
    color = if assigns[:color], do: "tw-text-#{assigns.color}", else: ""
    assigns = assign(assigns, color: color, animation: animation)

    ~H"""
    <i class={"inline-block fas fa-#{@icon} #{@color} tw-align-middle tw-text-2xl #{@animation}"}></i>
    """
  end
end
