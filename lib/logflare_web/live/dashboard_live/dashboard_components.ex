defmodule LogflareWeb.DashboardLive.DashboardComponents do
  use LogflareWeb, :html
  use LogflareWeb, :routes
  use Phoenix.Component

  alias Logflare.Auth
  alias Phoenix.LiveView.JS

  attr :user, Logflare.User, required: true

  def subhead(assigns) do
    assigns =
      assigns
      |> assign(:flag_multibackend, LogflareWeb.Utils.flag("multibackend", assigns.user))

    ~H"""
    <div class="subhead ">
      <div class="container mx-auto">
        <h5>~/logs</h5>
        <div class="log-settings">
          <ul>
            <li>
              <i class="fa fa-info-circle" aria-hidden="true"></i>
              <span>
                ingest API key
                <code class="pointer-cursor logflare-tooltip" id="api-key" phx-click={JS.dispatch("logflare:copy-to-clipboard", detail: %{text: @user.api_key})} data-showing-api-key="false" data-clipboard-text={@user.api_key} data-toggle="tooltip" data-placement="top" title="Copy this">
                  CLICK ME
                </code>
              </span>
            </li>
            <li>
              <.link href={~p"/access-tokens"}>
                <i class="fas fa-key"></i><span class="hide-on-mobile"> access tokens</span>
              </.link>
            </li>
            <li :if={@flag_multibackend}>
              <.link href={~p"/backends"}>
                <i class="fas fa-database"></i><span class="hide-on-mobile"> backends</span>
              </.link>
            </li>
            <li>
              <.link href={~p"/integrations/vercel/edit"}>
                ▲<span class="hide-on-mobile"> vercel
                  integration</span>
              </.link>
            </li>
            <li>
              <.link href={~p"/billing/edit"}>
                <i class="fas fa-money-bill"></i><span class="hide-on-mobile"> billing</span>
              </.link>
            </li>
            <li><a href="mailto:support@logflare.app?Subject=Logflare%20Help" target="_top"><i class="fas fa-question-circle"></i> <span class="hide-on-mobile">help</span></a></li>
          </ul>
        </div>
      </div>
    </div>
    """
  end

  def integrations(assigns) do
    ~H"""
    <div class="">
      <.integrations_list>
        <:heading>Integrations</:heading>
        <:item title="Cloudflare" link="https://cloudflareapps.com/apps/logflare" />
        <:item title="Vercel" link="https://docs.logflare.app/integrations/vercel/" />
        <:item title="Fly" link="https://github.com/Logflare/fly-log-shipper" />
        <:item title="Postgres FDW" link="https://docs.logflare.app/integrations/postgres-fdw" description="SQL" />
        <:item title="pino-logflare" link="https://docs.logflare.app/integrations/pino-logflare" description="Javascript" />
        <:item title="LoggerBackend" link="https://github.com/Logflare/logflare_logger_backend" description="Elixir" />
        <:item title="logflare_erl" link="https://github.com/Logflare/logflare_erl" description="Erlang" />
      </.integrations_list>

      <.link class="tw-text-white tw-text-sm" href="https://github.com/Logflare/logflare#integrations">View all integrations</.link>

      <.integrations_list>
        <:heading>Documentation</:heading>
        <:item title="docs.logflare.app" link="https://docs.logflare.app" />
        <:item title="OpenAPI" link={~p"/swaggerui"} />
      </.integrations_list>
    </div>
    """
  end

  slot :heading, required: true

  slot :item do
    attr :title, :string, required: true
    attr :link, :string, required: true
    attr :description, :string, required: false
  end

  def integrations_list(assigns) do
    ~H"""
    <h5 class="header-margin"><%= render_slot(@heading) %></h5>
    <ul class="tw-list-none tw-p-0 tw-m-0 tw-mb-3">
      <li :for={item <- @item} class="tw-px-5 tw-py-3 tw-border-b tw-border-gray-200 tw-bg-[#1d1d1d] tw-text-sm">
        <.link class="tw-text-white" href={item.link}><%= item.title %></.link>
        <span :if={item[:description]} class="tw-text-xs tw-block">
          <%= item.description %>
        </span>
      </li>
    </ul>
    """
  end

  def teams(assigns) do
    ~H"""
    <div>
      <h5 class="header-margin">Teams</h5>
      <ul class="list-unstyled">
        <li :if={@home_team} class="tw-mb-2">
          <strong :if={@current_team.id == @home_team.id}><%= @home_team.name %></strong>
          <.link :if={@current_team.id != @home_team.id} href={~p"/profile/switch?#{%{"user_id" => @home_team.user_id}}"} class="tw-text-white">
            <%= @home_team.name %>
          </.link>
          <small>home team</small>
        </li>

        <li :if={@home_team == nil} class="tw-mb-2">
          <.link href={~p"/account"} method="post" class="tw-text-white">
            Create your own Logflare account.
          </.link>
        </li>

        <%= if Enum.empty?(@team_users) do %>
          Other teams you are a member of will be listed here.
        <% end %>

        <li :for={team_user <- @team_users} :if={team_user.team.id != @current_team.id} class="tw-mb-2">
          <span :if={team_user.team_id == @current_team.id}><%= team_user.team.name %></span>
          <.link :if={team_user.team_id != @current_team.id} href={~p"/profile/switch?#{%{"user_id" => team_user.team.user_id, "team_user_id" => team_user.id}}"} class="tw-text-white">
            <%= team_user.team.name %>
          </.link>
        </li>
      </ul>
    </div>
    """
  end

  def members(assigns) do
    ~H"""
    <div>
      <h5 class="header-margin">Members</h5>
      <ul id="team-members" class="tw-mb-4 tw-list-none tw-p-0">
        <li class="tw-mb-2">
          <img class="rounded-circle" width="35" height="35" src={@user.image || Auth.gen_gravatar_link(@user.email)} alt={@user.name || @user.email} />
          <.link href={"mailto:#{@user.email}"} class="tw-text-white">
            <%= @user.name || @user.email %>
          </.link>
          <small><%= if true, do: "owner", else: "owner, you" %></small>
        </li>
        <li :for={member <- @team.team_users} :if={member.id != @user.id} class="tw-mb-2">
          <img class="rounded-circle" width="35" height="35" src={member.image || Auth.gen_gravatar_link(member.email)} alt={member.name || member.email} />
          <.link href={"mailto:#{member.email}"} class="tw-text-white">
            <%= member.name || member.email %>
          </.link>

          <.link href={~p"/profile/#{member.id}"} data-confirm="Delete member?" class="dashboard-links" method="delete">
            <i class="fa fa-trash"></i>
          </.link>
        </li>
      </ul>
      <.link href={~p"/account/edit#team-members"} class="tw-text-white tw-mt-2">
        Invite more team members.
      </.link>
    </div>
    """
  end

  def saved_searches(assigns) do
    ~H"""
    <div>
      <h5 class="header-margin">Saved Searches</h5>
      <%= if Enum.all?(@sources, &(Map.get(&1, :saved_searches) == [])) do %>
        Your saved searches will show up here. Save some searches!
      <% end %>
      <ul class="list-unstyled">
        <%= for source <- @sources, saved_search <- source.saved_searches do %>
          <li>
            <.link href={~p"/sources/#{source}/search?#{%{querystring: saved_search.querystring, tailing: saved_search.tailing}}"} class="tw-text-white">
              <%= source.name %>:<%= saved_search.querystring %>
            </.link>
            <.link href={~p"/sources/#{source}/saved-searches/#{saved_search}"} method="delete" class="dashboard-links">
              <i class="fa fa-trash"></i>
            </.link>
          </li>
        <% end %>
      </ul>
    </div>
    """
  end
end
