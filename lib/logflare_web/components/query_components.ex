defmodule LogflareWeb.QueryComponents do
  use Phoenix.Component

  alias LogflareWeb.Utils

  attr :bytes, :integer, default: nil

  def query_cost(assigns) do
    {size, unit} = Utils.humanize_bytes(assigns.bytes)

    assigns =
      assigns
      |> assign(unit: unit)
      |> assign(size: Decimal.from_float(size) |> Decimal.normalize())

    ~H"""
    <div :if={is_number(@bytes)} class="tw-text-sm">
      <%= @size %> <%= @unit %> processed
    </div>
    """
  end
end
