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

  @doc """
  Common subheader used across all pages
  """
  slot :path, required: true

  def subheader(assigns) do
    ~H"""
    <div class="subhead">
      <div class="container mx-auto tw-flex tw-flex-col tw-justify-between">
        <h5><%= render_slot(@path) %></h5>
        <div class="tw-flex  tw-flex-row tw-justify-end tw-gap-2">
          <%= render_slot(@inner_block) %>
        </div>
      </div>
    </div>
    """
  end

  @doc """
  Styled link, to be used in the page path header
  """
  attr :to, :string
  attr :live_patch, :boolean, default: false
  slot :inner_block, required: true

  def subheader_path_link(assigns) do
    ~H"""
    <%= if @live_patch do %>
      <%= live_patch to: @to, class: "tw-text-gray-600 tw-hover:text-black" do %>
        <%= render_slot(@inner_block) %>
      <% end %>
    <% else %>
      <%= Phoenix.HTML.Link.link to: @to, class: "tw-text-gray-600 tw-hover:text-black" do %>
        <%= render_slot(@inner_block) %>
      <% end %>
    <% end %>
    """
  end

  @doc """
  A subheader link, to be used together with the subheader component.
  Will be right aligned and placed one row below the subheader path heading.
  """
  attr :text, :string
  attr :to, :string
  attr :fa_icon, :string
  attr :live_patch, :boolean, default: false

  def subheader_link(assigns) do
    ~H"""
    <% attrs = [
      to: @to,
      class: "tw-text-black tw-p-1 tw-flex tw-gap-1 tw-items-center tw-justify-center"
    ]

    icon_class = "inline-block h-3 w-3 fas fa-#{@fa_icon}" %>
    <%= if @live_patch do %>
      <%= live_patch attrs do %>
        <i :if={@fa_icon} class={icon_class}></i><span> <%= @text %></span>
      <% end %>
    <% else %>
      <%= Phoenix.HTML.Link.link attrs do %>
        <i :if={@fa_icon} class={icon_class}></i><span> <%= @text %></span>
      <% end %>
    <% end %>
    """
  end

  attr :text, :string, required: true

  def header_with_anchor(assigns) do
    assigns =
      assign(
        assigns,
        :anchor,
        assigns.text
        |> String.downcase()
        |> String.replace(" ", "-")
      )

    ~H"""
    <h5 id={@anchor} class="tw-mb-2 tw-mt-4 tw-text-white scroll-margin">
      <%= @text %> <%= Phoenix.HTML.Link.link("#", to: "#" <> @anchor) %>
    </h5>
    """
  end
end
