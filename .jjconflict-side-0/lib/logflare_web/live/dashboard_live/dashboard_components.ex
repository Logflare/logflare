defmodule LogflareWeb.DashboardLive.DashboardComponents do
  use LogflareWeb, :html
  use LogflareWeb, :routes
  use Phoenix.Component

  alias Logflare.Auth
  alias Phoenix.LiveView.JS

  attr :user, Logflare.User, required: true
  attr :team, Logflare.Teams.Team, required: true

  def subhead(assigns) do
    assigns =
      assigns
      |> assign(:flag_multibackend, Logflare.Utils.flag("multibackend", assigns.user))
      |> assign(:flag_key_values, Logflare.Utils.flag("key_values", assigns.user))

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
              <.team_link team={@team} href={~p"/access-tokens"}>
                <i class="fas fa-key"></i><span class="hide-on-mobile"> access tokens</span>
              </.team_link>
            </li>
            <li :if={@flag_multibackend}>
              <.team_link team={@team} href={~p"/backends"}>
                <i class="fas fa-database"></i><span class="hide-on-mobile"> backends</span>
              </.team_link>
            </li>
            <li :if={@flag_key_values}>
              <.team_link team={@team} href={~p"/key-values"}>
                <i class="fas fa-th-list"></i><span class="hide-on-mobile"> key values</span>
              </.team_link>
            </li>
            <li>
              <.team_link team={@team} href={~p"/integrations/vercel/edit"}>
                ▲<span class="hide-on-mobile"> vercel
                  integration</span>
              </.team_link>
            </li>
            <li>
              <.team_link team={@team} href={~p"/billing/edit"}>
                <i class="fas fa-money-bill"></i><span class="hide-on-mobile"> billing</span>
              </.team_link>
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
    <h5 class="header-margin">{render_slot(@heading)}</h5>
    <ul class="tw-list-none tw-p-0 tw-m-0 tw-mb-3">
      <li :for={item <- @item} class="tw-px-5 tw-py-3 tw-border-b tw-border-gray-200 tw-bg-[#1d1d1d] tw-text-sm">
        <.link class="tw-text-white" href={item.link}>{item.title}</.link>
        <span :if={item[:description]} class="tw-text-xs tw-block">
          {item.description}
        </span>
      </li>
    </ul>
    """
  end

  attr :user, Logflare.User, required: true
  attr :team, Logflare.Teams.Team, required: true
  attr :team_user, Logflare.TeamUsers.TeamUser, default: nil

  def members(assigns) do
    ~H"""
    <div id="members">
      <h5 class="header-margin">Members</h5>
      <ul id="team-members" class="tw-mb-4 tw-list-none tw-p-0">
        <li class="tw-mb-2">
          <img class="rounded-circle" width="35" height="35" src={@user.image || Auth.gen_gravatar_link(@user.email)} alt={@user.name || @user.email} />
          <.link href={"mailto:#{@user.email}"} class="tw-text-white">
            {@user.name || @user.email}
          </.link>
          <small>{if @team_user, do: "owner", else: "owner, you"}</small>
        </li>
        <li :for={member <- @team.team_users} class="tw-mb-2">
          <img class="rounded-circle" width="35" height="35" src={member.image || Auth.gen_gravatar_link(member.email)} alt={member.name || member.email} />
          <.link href={"mailto:#{member.email}"} class="tw-text-white">
            {member.name || member.email}
          </.link>
          <span :if={current_team_user?(member, @team_user)}>you</span>
        </li>
      </ul>
      <.team_link :if={is_nil(@team_user)} team={@team} href={~p"/account/edit#team-members"} class="tw-text-white tw-mt-2">
        Invite more team members.
      </.team_link>
    </div>
    """
  end

  defp current_team_user?(_member, nil), do: false
  defp current_team_user?(member, team_user), do: member.provider_uid == team_user.provider_uid
end
