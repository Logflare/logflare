defmodule LogflareWeb.DashboardLive do
  use LogflareWeb, :live_view

  alias Logflare.{Billing, Sources, Teams, TeamUser, User, Users}
  alias LogflareWeb.DashboardLive.{DashboardComponents, DashboardSourceComponents}
  alias LogflareWeb.Helpers.Forms

  @impl true
  def mount(_, %{"user_id" => user_id} = session, socket) do
    user =
      Users.get_by_and_preload(id: user_id)
      |> Users.preload_sources()
      |> Users.preload_team()

    sources = Sources.preload_for_dashboard(user.sources)

    plan = Billing.get_plan_by_user(user)

    socket =
      socket
      |> assign(:user, user)
      |> assign(:plan, plan)
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

  @impl true
  def handle_event("toggle_favorite", %{"id" => id}, socket) do
    %{user: user} = socket.assigns

    with source <- Sources.get_by_and_preload(id: id),
         true <- LogflareWeb.Plugs.SetVerifySource.verify_source_for_user(source, user),
         {:ok, source} <-
           Sources.update_source_by_user(source, %{"favorite" => !source.favorite}) do
      {:noreply, socket |> put_flash(:info, "Source updated!")}
    else
      _ -> {:noreply, socket |> put_flash(:error, "Something went wrong!")}
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div>
      <DashboardComponents.subhead user={@user} />
      <div class="tw-max-w-[95%] tw-mx-auto">
        <div class="tw-grid tw-grid-cols-12 tw-gap-8 tw-px-[15px] tw-mt-[50px]">
          <div class="tw-col-span-3">
            <.saved_searches sources={@sources} />
            <DashboardComponents.teams current_team={@team} home_team={@user.team} team_users={@team_users} />
            <DashboardComponents.members user={@user} team={@team} />
          </div>
          <div class="tw-col-span-7">
            <.source_list sources={@sources} plan={@plan} />
          </div>
          <div class="tw-col-span-2">
            <DashboardComponents.integrations />
          </div>
        </div>
      </div>
    </div>
    """
  end

  def source_list(assigns) do
    ~H"""
    <div id="source-list" phx-hook="FormatTimestamps">
      <div class="tw-mb-3 tw-flex tw-justify-end">
        <.link href={~p"/query"} class="btn btn-primary btn-sm">
          Run a query
        </.link>
        <.link href={~p"/sources/new"} class="btn btn-primary btn-sm">
          New source
        </.link>
      </div>
      <ul class="list-group">
        <%= if Enum.empty?(@sources) do %>
          <li class="list-group-item">You don't have any sources!</li>
          <li class="list-group-item">Sources are where your log events go.</li>
          <li class="list-group-item">Create one now!</li>
        <% end %>
        <%= for {service_name, sources} <- Enum.group_by(@sources, fn s -> s.service_name end) |> Enum.reverse() do %>
          <li :if={service_name != nil} class="list-group-item !tw-pb-0"><%= Forms.section_header(service_name) %></li>
          <li :if={service_name == nil} class="list-group-item">
            <hr />
          </li>
          <li :for={source <- sources} class="list-group-item">
            <div class="favorite float-left tw-cursor-pointer tw-text-yellow-200" phx-click="toggle_favorite" phx-value-id={source.id}>
              <span>
                <i class={[if(source.favorite, do: "fas", else: "far"), "fa-star "]} />
              </span>
            </div>
            <div>
              <div class="float-right">
                <.link href={~p"/sources/#{source}/edit"} class="dashboard-links">
                  <i class="fas fa-edit"></i>
                </.link>
              </div>
              <div class="source-link word-break-all">
                <.link href={~p"/sources/#{source}"} class="tw-text-white"><%= source.name %></.link>
                <span id={source.token}>
                  <small class="my-badge my-badge-info">
                    <%= source.metrics.inserts_string %>
                  </small>
                </span>
              </div>
            </div>
            <DashboardSourceComponents.source_metadata source={source} plan={@plan} />
          </li>
        <% end %>
      </ul>
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
