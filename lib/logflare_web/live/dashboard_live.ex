defmodule LogflareWeb.DashboardLive do
  use LogflareWeb, :live_view

  alias Logflare.{Repo, Sources, Teams, TeamUser, User, Users}
  alias LogflareWeb.DashboardLive.DashboardComponents

  def mount(_, %{"user_id" => user_id} = session, socket) do
    session |> dbg

    user =
      Users.get_by_and_preload(id: user_id)
      |> Users.preload_sources()
      |> Users.preload_team()

    sources = Sources.preload_for_dashboard(user.sources)

    socket =
      socket
      |> assign(:user, user)
      |> assign_teams(session["team_user_id"])
      |> assign(:sources, sources)

    {:ok, socket}
  end

  def assign_teams(socket, nil) do
    %{user: user} = socket.assigns

    # this is wrong I think
    team_users = Logflare.TeamUsers.list_team_users_by_and_preload(email: user.email)

    assign(socket, team: user.team |> Teams.preload_team_users(), team_users: team_users)
  end

  def assign_teams(socket, team_user_id) do
    %{user: user} = socket.assigns

    team_user = TeamUsers.get_team_user_and_preload(team_user_id)
    team_users = Logflare.TeamUsers.list_team_users_by_and_preload(email: user.email)

    socket
    |> assign(
      team_users: team_users,
      team: team_user.team |> Teams.preload_team_users()
    )
  end

  def render(assigns) do
    ~H"""
    <div class="">
      <.subhead user={@user} />
      <div class="tw-flex tw-mt-[50px] tw-mx-[40px] tw-px-4">
        <div class="w-1/4 tw-px-4">
          <.saved_searches sources={@sources} />
          <DashboardComponents.teams current_team={@team} home_team={@user.team} team_users={@team_users} />
          <DashboardComponents.members user={@user} team={@team} />
        </div>
        <div class="tw-grow">
          sources
        </div>
        <div class="tw-w-1/4">
          <DashboardComponents.integrations />
        </div>
      </div>
    </div>
    """
  end

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

  def saved_searches(assigns) do
    ~H"""
    <div>
      <h5 class="header-margin">Saved Searches</h5>
      <ul class="list-unstyled">
        <%= if Enum.all?(@sources, &(Map.get(&1, :saved_searches) == [])) do %>
          Your saved searches will show up here. Save some searches!
        <% end %>
        <%= for source <- @sources, saved_search <- source.saved_searches do %>
          <li>
            <.link navigate={~p"/sources/#{source}/search?#{%{querystring: saved_search.querystring, tailing: saved_search.tailing}}"}>
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

  def teams(assigns) do
    ~H"""
    <div>
      current: <%= @current_team.name %>
      <h5 class="header-margin">Teams</h5>
      <ul class="list-unstyled">
        <li :if={@home_team}>
          <strong :if={@current_team.id == @home_team.id}><%= @home_team.name %></strong>
          <.link :if={@current_team.id != @home_team.id} navigate={~p"/profile/switch?#{%{"user_id" => @home_team.user_id, "redirect_to" => "/dashboard_new"}}"}>
            <%= @home_team.name %>
          </.link>
          <small>home team</small>
        </li>

        <li :if={@home_team == nil}>
          <.link href={~p"/account"} method="post">
            Create your own Logflare account.
          </.link>
        </li>

        <%= if Enum.empty?(@team_users) do %>
          Other teams you are a member of will be listed here.
        <% end %>

        <li :for={team_user <- @team_users} :if={team_user.team.id != @current_team.id}>
          <li>
            <span :if={team_user.team_id == @current_team.id}><%= team_user.team.name %></span>
            <.link :if={team_user.team_id != @current_team.id} navigate={~p"/profile/switch?#{%{"user_id" => team_user.team.user_id, "team_user_id" => team_user.id, "redirect_to" => "/dashboard_new"}}"}>
              <%= team_user.team.name %>
            </.link>
          </li>
        </li>
      </ul>
    </div>
    """
  end

  def members(assigns) do
    ~H"""
    <div>
      <h5 class="header-margin">Members</h5>
      <ul id="team-members" class="tw-mb-1 tw-list-none tw-p-0">
        <li>
          <img class="rounded-circle" width="35" height="35" src={@user.image || Auth.gen_gravatar_link(@user.email)} alt={@user.name || @user.email} />
          <.link href={"mailto:#{@user.email}"}>
            <%= @user.name || @user.email %>
          </.link>
          <small><%= if true, do: "owner", else: "owner, you" %></small>
        </li>
        <li :for={member <- @team.team_users} :if={member.id != @user.id}>
          <img class="rounded-circle" width="35" height="35" src={member.image || Auth.gen_gravatar_link(member.email)} alt={member.name || member.email} />
          <.link href={"mailto:#{member.email}"}>
            <%= member.name || member.email %>
          </.link>

          <.link href={~p"/profile/#{member.id}"} prompt="Delete member?" class="dashboard-links" method="delete">
            <i class="fa fa-trash"></i>
          </.link>
        </li>
      </ul>
      <.link href={~p"/account/edit#team-members"}>
        Invite more team members.
      </.link>
    </div>
    """
  end
end
