defmodule LogflareWeb.CoreComponents do
  @moduledoc """
  Global components.
  """
  use Phoenix.Component

  @doc "Alert the user of something"
  attr :variant, :string,
    values: ["primary", "secondary", "success", "danger", "warning", "info", "light", "dark"]

  slot :inner_block, required: true

  def alert(assigns) do
    ~H"""
    <div class={"alert alert-#{@variant}"} role="alert">
      <%= render_slot(@inner_block) %>
    </div>
    """
  end

  slot :path, required: true

  def subheader(assigns) do
    ~H"""
    <div class="subhead ">
      <div class="container mx-auto tw-flex tw-justify-between">
        <h5><%= render_slot(@path) %></h5>
        <div class="tw-flex tw-flex-row tw-justify-end tw-gap-2">
          <%= render_slot(@inner_block) %>
        </div>
      </div>
    </div>
    """
  end
end
