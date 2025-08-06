defmodule LogflareWeb.CoreComponents do
  @moduledoc """
  Global components.
  """
  use LogflareWeb, :routes
  use Phoenix.Component

  @doc "Alert the user of something"
  attr :variant, :string,
    values: ["primary", "secondary", "success", "danger", "warning", "info", "light", "dark"]

  attr :class, :string, required: false, default: ""
  slot :inner_block, required: true

  def alert(assigns) do
    ~H"""
    <div class={["alert alert-#{@variant}", @class]} role="alert">
      <%= render_slot(@inner_block) %>
    </div>
    """
  end

  @doc "button"
  attr :variant, :string,
    values: ["primary", "secondary", "success", "danger", "warning", "info", "light", "dark"]

  attr :class, :string, default: ""
  attr :rest, :global
  slot :inner_block, required: true

  def button(assigns) do
    ~H"""
    <button class={"btn btn-#{@variant} #{@class}"} type="button" {@rest}>
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
    <.dynamic_link to={@to} patch={@live_patch} class="tw-text-gray-600 tw-hover:text-black">
      <%= render_slot(@inner_block) %>
    </.dynamic_link>
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
  attr :external, :boolean, default: false

  def subheader_link(assigns) do
    ~H"""
    <.dynamic_link to={@to} patch={@live_patch} external={@external} class="tw-text-black tw-p-1 tw-flex tw-gap-1 tw-items-center tw-justify-center">
      <i :if={@fa_icon} class={"inline-block h-3 w-3 fas fa-#{@fa_icon}"}></i><span> <%= @text %></span>
    </.dynamic_link>
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

  attr :to, :string
  attr :patch, :boolean
  attr :attrs, :global
  slot :inner_block, required: true
  attr :external, :boolean, default: false

  defp dynamic_link(assigns) do
    if assigns.external do
      ~H"""
      <a href={@to} target="_blank" rel="noopener noreferrer" {@attrs}>
        <%= render_slot(@inner_block) %>
      </a>
      """
    else
      link_type =
        if assigns.patch do
          :patch
        else
          :navigate
        end

      assigns = assign(assigns, :to, %{link_type => assigns.to})

      ~H"""
      <.link {@to} {@attrs}><%= render_slot(@inner_block) %></.link>
      """
    end
  end

  attr :log_event_id, :string
  attr :timestamp, :integer
  attr :source, Logflare.Source
  attr :lql, :string
  attr :label, :string, default: "permalink"
  attr :class, :string, default: "group-hover:tw-visible tw-invisible"

  def log_event_permalink(assigns) do
    ~H"""
    <.link class={@class} target="_blank" href={~p"/sources/#{@source.id}/event?#{%{uuid: @log_event_id, timestamp: Logflare.Utils.iso_timestamp(@timestamp), lql: @lql}}"}><%= @label %></.link>
    """
  end
end
