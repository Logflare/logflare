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

  @doc "button"
  attr :variant, :string,
    values: ["primary", "secondary", "success", "danger", "warning", "info", "light", "dark"]

  slot :inner_block, required: true

  def button(assigns) do
    ~H"""
    <button class={"btn btn-#{@variant}"} type="button">
      <%= render_slot(@inner_block) %>
    </button>
    """
  end

  slot :path, required: true

  def subheader(assigns) do
    ~H"""
    <div class="subhead">
      <div class="container pb-1 mx-auto tw-flex tw-flex-col tw-justify-between">
        <h5><%= render_slot(@path) %></h5>
        <div class="tw-flex  tw-flex-row tw-justify-end tw-gap-2">
          <%= render_slot(@inner_block) %>
        </div>
      </div>
    </div>
    """
  end

  @doc """
  A subheader link, to be used together with the subheader component.
  """
  attr :text, :string
  attr :to, :string
  attr :fa_icon, :string

  def subheader_link(assigns) do
    ~H"""
    <%= Phoenix.HTML.Link.link to: @to, class: "tw-text-black tw-p-1 tw-flex tw-gap-1 tw-items-center tw-justify-center" do %>
      <i :if={@fa_icon} class={"inline-block h-3 w-3 fas fa-#{@fa_icon}"}></i><span>
      <%= @text %>
      </span>
    <% end %>
    """
  end
end
