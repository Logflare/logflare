defmodule LogflareWeb.Backends.Components do
  use Phoenix.Component

  import PhoenixHTMLHelpers.Form
  import Phoenix.HTML.Form
  alias Logflare.Backends.Adaptor.HttpBased.Headers

  # TODO: Extract common components (e.g. inputs) from backend_form

  attr :form, :any, required: true, doc: "the backend's :config inner form"

  @doc """
  Key/value inputs for a backend's user-supplied HTTP headers.

  Renders one blank row beyond the stored headers so another can always be added. Stored
  values are masked, and the hidden `header{i}_stored_key` carries the original key so
  `BackendsLive.header_value/3` can restore the real value on submit.
  """
  def header_inputs(assigns) do
    stored = input_value(assigns.form, :headers) || %{}

    fields =
      stored
      |> Enum.sort_by(fn {key, _value} -> key end)
      |> Stream.concat(Stream.repeatedly(fn -> {"", ""} end))
      |> Enum.take(max(map_size(stored) + 1, 2))

    assigns = assign(assigns, :fields, fields)

    ~H"""
    <div :for={{{key, value}, i} <- Enum.with_index(@fields, 1)} class="form-group">
      {label(@form, "header#{i}_key", "Custom header #{i} - Key")}
      {text_input(@form, "header#{i}_key", value: key, class: "form-control")}
      {label(@form, "header#{i}_value", "Custom header #{i} - Value")}
      {text_input(@form, "header#{i}_value", value: Headers.mask_value(value), class: "form-control")}
      {hidden_input(@form, "header#{i}_stored_key", value: key)}
    </div>
    """
  end

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

  defp status_error_message(reason)
       when reason in [:source_required, {:error, :source_required}] do
    "Attach a source or add a drain rule before testing this connection."
  end

  defp status_error_message(_reason) do
    LogflareWeb.QueryErrorHelpers.generic_query_error_message()
  end
end
