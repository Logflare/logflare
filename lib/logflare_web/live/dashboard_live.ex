defmodule LogflareWeb.DashboardLive do
  alias Logflare.Repo
  use LogflareWeb, :live_view

  alias Logflare.{Billing, Sources, Teams, TeamUsers, Users}
  alias LogflareWeb.DashboardLive.{DashboardComponents, DashboardSourceComponents}
  alias LogflareWeb.Helpers.Forms

  @impl true
  def mount(_, %{"user_id" => user_id} = session, socket) do
    socket =
      socket
      |> assign(:user, Users.get_by_and_preload(id: user_id))
      |> assign_new(:team, fn %{user: user} ->
        Teams.get_team_by(user_id: user.id) |> Teams.preload_team_users()
      end)
      |> assign_new(:sources, fn %{user: user} ->
        user
        |> Sources.list_sources_by_user()
        |> Sources.preload_for_dashboard()
      end)
      |> assign_new(:plan, fn %{user: user} -> Billing.get_plan_by_user(user) end)
      |> assign_teams(session["team_user_id"])
      |> assign(:fade_in, false)

    if connected?(socket) do
      Enum.each(
        socket.assigns.sources,
        &Logflare.Sources.Source.ChannelTopics.subscribe_dashboard(&1.token)
      )
    end

    {:ok, socket}
  end

  @doc """
  Assigns teams and members.

  If the user is signed in as `team_user` then `user` will be the team owner.
  """
  def assign_teams(socket, nil) do
    %{user: user} = socket.assigns

    home_team = user.team |> Logflare.Repo.preload(:user)
    team_users = Logflare.TeamUsers.list_team_users_by_and_preload(email: user.email)

    assign(socket,
      home_team: home_team,
      team_user: nil,
      team_users: team_users
    )
  end

  def assign_teams(socket, team_user_id) do
    team_user = TeamUsers.get_team_user_and_preload(team_user_id)
    home_team = Teams.get_home_team(team_user)
    team_users = TeamUsers.list_team_users_by_and_preload(provider_uid: team_user.provider_uid)

    socket
    |> assign(
      home_team: home_team,
      team_user: team_user,
      team_users: team_users
    )
  end

  @impl true
  def handle_event("toggle_favorite", %{"id" => id} = params, socket) do
    %{user: user} = socket.assigns
    favorite = Map.has_key?(params, "favorite")

    with source <- Sources.get_by_and_preload(id: id),
         true <- LogflareWeb.Plugs.SetVerifySource.verify_source_for_user(source, user),
         {:ok, _source} <- Sources.update_source_by_user(source, %{"favorite" => favorite}) do
      sources =
        Repo.reload(socket.assigns.sources)
        |> Sources.preload_for_dashboard()

      {:noreply, assign(socket, sources: sources)}
    else
      _ -> {:noreply, socket |> put_flash(:error, "Something went wrong!")}
    end
  end

  def handle_info(%Phoenix.Socket.Broadcast{event: "buffer"} = broadcast, socket) do
    %{payload: payload} = broadcast

    {:noreply,
     update_source_metrics(socket, payload.source_id, fn metrics ->
       %{metrics | buffer: payload.buffer}
     end)}
  end

  def handle_info(%Phoenix.Socket.Broadcast{event: "rate"} = broadcast, socket) do
    %{payload: payload} = broadcast

    {:noreply,
     update_source_metrics(socket, payload.source_token, fn metrics ->
       %{
         metrics
         | avg: payload.average_rate,
           max: payload.max_rate,
           rate: payload.last_rate
       }
     end)}
  end

  def handle_info(%Phoenix.Socket.Broadcast{event: "log_count"} = broadcast, socket) do
    %{payload: payload} = broadcast

    socket =
      update_source_metrics(socket, payload.source_token, fn metrics ->
        %{
          metrics
          | latest: DateTime.utc_now() |> DateTime.to_unix(:microsecond),
            inserts_string: Number.Delimit.number_to_delimited(payload.log_count)
        }
      end)
      |> assign(fade_in: true)

    {:noreply, socket}
  end

  def update_source_metrics(socket, id, update_fn) when is_integer(id) do
    case Enum.find(socket.assigns.sources, fn source -> source.id == id end) do
      nil ->
        socket

      source ->
        update_source_metrics(socket, source.token, update_fn)
    end
  end

  def update_source_metrics(socket, token, update_fn) when is_atom(token) do
    sources =
      Enum.map(socket.assigns.sources, fn source ->
        if source.token == token do
          metrics = update_fn.(source.metrics)
          %{source | metrics: metrics}
        else
          source
        end
      end)

    assign(socket, sources: sources)
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div>
      <DashboardComponents.subhead user={@user} />
      <div class="tw-max-w-[95%] tw-mx-auto">
        <div class="lg:tw-grid tw-grid-cols-12 tw-gap-8 tw-px-[15px] tw-mt-[50px]">
          <div class="tw-col-span-3">
            <DashboardComponents.saved_searches sources={@sources} />
            <DashboardComponents.teams current_team={@team} home_team={@home_team} team_users={@team_users} />
            <DashboardComponents.members user={@user} team={@team} team_user={@team_user} />
          </div>
          <div class="tw-col-span-7">
            <.source_list sources={@sources} plan={@plan} fade_in={@fade_in} />
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
        <%= for {service_name, sources} <- grouped_sources(@sources) do %>
          <li :if={service_name != nil} class="list-group-item"><Forms.section_header text={service_name} /></li>
          <li :if={service_name == nil} class="list-group-item">
            <hr />
          </li>
          <li :for={source <- sources} class="list-group-item" id={to_string(source.token)}>
            <div class="favorite float-left tw-cursor-pointer tw-text-yellow-200" phx-click="toggle_favorite" phx-value-favorite={!source.favorite} phx-value-id={source.id}>
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
                <span>
                  <small class="my-badge my-badge-info tw-transition-colors tw-ease-in" id={"#{source.id}-inserts-#{source.metrics.inserts_string}"} phx-mounted={if(@fade_in, do: JS.transition("tw-bg-blue-500", time: 500))}>
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

  # groups services by name, ungrouped sources last.
  defp grouped_sources(sources) do
    sources |> Enum.group_by(fn source -> source.service_name end) |> Enum.reverse()
  end
end
