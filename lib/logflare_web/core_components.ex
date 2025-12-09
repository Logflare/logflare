defmodule LogflareWeb.CoreComponents do
  @moduledoc """
  Global components.
  """
  use LogflareWeb, :routes
  use Phoenix.Component

  alias Phoenix.LiveView.JS
  alias Logflare.Teams.TeamContext

  @doc "Alert the user of something"
  attr :variant, :string,
    values: ["primary", "secondary", "success", "danger", "warning", "info", "light", "dark"]

  attr :class, :string, required: false, default: ""
  slot :inner_block, required: true

  def alert(assigns) do
    ~H"""
    <div class={["alert alert-#{@variant}", @class]} role="alert">
      {render_slot(@inner_block)}
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
      {render_slot(@inner_block)}
    </button>
    """
  end

  attr :id, :string, required: false
  attr :disabled, :boolean, default: false
  slot :inner_block, required: true

  slot :menu_item, required: true do
    attr :heading, :string
  end

  def button_dropdown(assigns) do
    assigns = assigns |> assign_new(:id, fn -> "button-dropdown-#{UUID.uuid4()}" end)

    ~H"""
    <div class="tw-relative" id={@id}>
      <button type="button" class="btn btn-primary" phx-click={JS.toggle(to: "##{@id} ul")} disabled={@disabled}>
        {render_slot(@inner_block)}
      </button>
      <ul phx-click-away={JS.hide()} style="display: none;" class="tw-absolute tw-left-0 tw-m-0 tw-px-0 tw-bottom-full tw-bg-white tw-rounded-md tw-border tw-border-gray-300 tw-shadow tw-py-2 tw-min-w-[11rem] tw-list-none tw-z-10">
        <%= for menu_item <- @menu_item do %>
          <li :if={menu_item[:heading]} class="tw-mt-2 first:tw-mt-0 tw-border-0 tw-border-t first:tw-border-t-0 tw-border-solid tw-border-gray-200 tw-px-3 tw-pt-2 tw-pb-1 tw-text-xs tw-font-semibold tw-text-gray-500 tw-uppercase">
            {menu_item.heading}
          </li>
          <li class="tw-block tw-px-3 tw-py-2 tw-text-gray-800 tw-no-underline hover:tw-bg-gray-100">
            {render_slot(menu_item)}
          </li>
        <% end %>
      </ul>
    </div>
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
        <h5>{render_slot(@path)}</h5>
        <div class="tw-flex  tw-flex-row tw-justify-end tw-gap-2">
          {render_slot(@inner_block)}
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
  attr :team, Logflare.Teams.Team, default: nil
  slot :inner_block, required: true

  def subheader_path_link(assigns) do
    ~H"""
    <.dynamic_link to={LogflareWeb.Utils.with_team_param(@to, @team)} patch={@live_patch} class="tw-text-gray-600 tw-hover:text-black">
      {render_slot(@inner_block)}
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
  attr :team, :any, default: nil

  def subheader_link(assigns) do
    ~H"""
    <.dynamic_link to={@to} patch={@live_patch} external={@external} team={@team} class="tw-text-black tw-p-1 tw-flex tw-gap-1 tw-items-center tw-justify-center">
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
      {@text} <.link href={"#" <> @anchor}>#</.link>
    </h5>
    """
  end

  attr :to, :string
  attr :patch, :boolean
  attr :attrs, :global
  slot :inner_block, required: true
  attr :external, :boolean, default: false
  attr :team, :any, default: nil

  defp dynamic_link(assigns) do
    if assigns.external do
      ~H"""
      <a href={@to} target="_blank" rel="noopener noreferrer" {@attrs}>
        {render_slot(@inner_block)}
      </a>
      """
    else
      link_type =
        if assigns.patch do
          :patch
        else
          :navigate
        end

      to_with_team = LogflareWeb.Utils.with_team_param(assigns.to, assigns.team)
      assigns = assign(assigns, :to, %{link_type => to_with_team})

      ~H"""
      <.link {@to} {@attrs}>{render_slot(@inner_block)}</.link>
      """
    end
  end

  attr :log_event_id, :string
  attr :timestamp, :integer
  attr :source, Logflare.Source
  attr :lql, :string
  attr :label, :string, default: "permalink"
  attr :class, :string, default: "group-hover:tw-visible tw-invisible"
  attr :icon, :string, default: nil

  def log_event_permalink(assigns) do
    ~H"""
    <.link class={@class} target="_blank" href={~p"/sources/#{@source.id}/event?#{%{uuid: @log_event_id, timestamp: Logflare.Utils.iso_timestamp(@timestamp), lql: @lql}}"}>
      <i :if={@icon} class={@icon <> " tw-mr-1 tw-w-2"}></i>
      {@label}
    </.link>
    """
  end

  @doc """
  Team switcher dropdown for the navbar.
  Displays the current team and allows switching between teams.

  ## Examples

      <.team_switcher teams={@teams} team_context={team_context} current_path={@conn.request_path}  />

  """
  attr :team_context, TeamContext, required: true
  attr :teams, :list, required: true
  attr :current_path, :string, required: true

  def team_switcher(assigns) do
    assigns =
      assigns
      |> assign(:has_multiple_teams, length(assigns.teams) > 1)
      |> assign(:selected_class, fn team_id ->
        if team_id == assigns.team_context.team.id, do: "tw-font-bold tw-text-neutral-900/60"
      end)

    ~H"""
    <li class="nav-item" id="team-switcher">
      <span :if={not @has_multiple_teams} class="tw-font-bold tw-text-black/60">
        {@team_context.team.name}
      </span>
      <a :if={@has_multiple_teams} class="tw-font-bold tw-text-neutral-900/60 nav-link dropdown-toggle" href="#" id="teamDropdown" role="button" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">
        {@team_context.team.name}
      </a>
      <div :if={@has_multiple_teams} class="dropdown-menu dropdown-menu-right" aria-labelledby="teamDropdown">
        <%= for team <- @teams do %>
          <.team_link team={team} href={@current_path} class={["dropdown-item tw-flex tw-items-center tw-gap-2", @selected_class.(team.id)]}>
            <span>{team.name}</span>
            <span :if={TeamContext.home_team?(team, @team_context)} class="tw-text-sm tw-self-end">home team</span>
          </.team_link>
        <% end %>
      </div>
    </li>
    """
  end

  @doc """
  Generate a link with a team_id param.

  ## Examples

      <.team_link href={~p"/dashboard"} team={@team}>Dashboard</.team_link>

  """
  attr :href, :string, default: nil
  attr :navigate, :string, default: nil
  attr :patch, :string, default: nil
  attr :team, :any, required: true
  attr :rest, :global
  slot :inner_block, required: true

  def team_link(assigns) do
    nav_attrs = [:navigate, :patch, :href]

    nav_assign =
      for key <- nav_attrs,
          value = Map.get(assigns, key),
          into: %{} do
        {key, LogflareWeb.Utils.with_team_param(value, assigns.team)}
      end

    assigns
    |> Map.merge(nav_assign)
    |> link()
  end
end
