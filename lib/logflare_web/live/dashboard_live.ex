defmodule LogflareWeb.DashboardLive do
  alias Logflare.Repo
  use LogflareWeb, :live_view

  alias Logflare.Billing
  alias Logflare.Sources
  alias Logflare.Teams
  alias LogflareWeb.DashboardLive.DashboardComponents
  alias LogflareWeb.DashboardLive.DashboardSourceComponents
  alias LogflareWeb.Helpers.Forms
  alias LogflareWeb.ModalLiveHelpers

  @impl true
  def mount(_, _session, socket) do
    %{user: user} = socket.assigns

    socket =
      socket
      |> assign(
        :sources,
        user |> Sources.list_sources_by_user() |> Sources.preload_for_dashboard()
      )
      |> assign_new(:source_metrics, fn %{sources: sources} ->
        sources
        |> Enum.into(%{}, fn source ->
          {to_string(source.token), %{metrics: source.metrics, updated_at: source.updated_at}}
        end)
      end)
      |> assign(:plan, Billing.get_plan_by_user(user))
      |> assign(:fade_in, false)
      |> assign(:show_modal, false)

    if connected?(socket) do
      %{user: user} = socket.assigns
      Logflare.Sources.UserMetricsPoller.track(self(), user.id)
      Phoenix.PubSub.subscribe(Logflare.PubSub, "dashboard_user_metrics:#{user.id}")
    end

    {:ok, socket}
  end

  @impl true
  def handle_event("toggle_favorite", %{"id" => id} = params, socket) do
    %{user: user} = socket.assigns
    favorite = Map.has_key?(params, "favorite")

    with source <- Sources.Cache.get_by_and_preload(id: id, user_id: user.id),
         {:ok, _source} <- Sources.update_source_by_user(source, %{"favorite" => favorite}) do
      sources =
        Repo.reload(socket.assigns.sources)
        |> Sources.preload_for_dashboard()

      {:noreply, assign(socket, sources: sources)}
    else
      _ -> {:noreply, socket |> put_flash(:error, "Something went wrong!")}
    end
  end

  def handle_event("visibility_change", %{"visibility" => "hidden"}, socket) do
    %{user: user} = socket.assigns

    Logflare.Sources.UserMetricsPoller.untrack(self(), user.id)
    {:noreply, socket}
  end

  def handle_event("visibility_change", %{"visibility" => "visible"}, socket) do
    %{user: user} = socket.assigns

    Logflare.Sources.UserMetricsPoller.track(self(), user.id)
    {:noreply, socket}
  end

  @impl true
  def handle_info({:set_flash, {type, message}}, socket) do
    {:noreply, put_flash(socket, type, message)}
  end

  @impl true
  def handle_info({:metrics_update, payload}, socket) do
    socket =
      payload
      |> Enum.reduce(socket, fn {token, metrics}, socket ->
        update_source_metrics(socket, to_string(token), metrics)
      end)

    {:noreply, assign(socket, fade_in: true)}
  end

  @spec update_source_metrics(Phoenix.LiveView.Socket.t(), String.t(), map()) ::
          Phoenix.LiveView.Socket.t()
  def update_source_metrics(socket, token, attrs) when is_binary(token) do
    source_metrics =
      update_in(socket.assigns.source_metrics, [Access.key(token), :metrics], fn metrics ->
        Map.merge(metrics, attrs)
      end)

    assign(socket, source_metrics: source_metrics)
  end

  @impl true
  def render(assigns) do
    ~H"""
    <%= if @show_modal do %>
      {ModalLiveHelpers.live_modal(@modal.body.module_or_template,
        id: @modal.body.id,
        title: @modal.body.title,
        params: @modal.params,
        user: @user,
        team: @team,
        close: @modal.body[:close],
        return_to: @modal.body.return_to
      )}
    <% end %>
    <div id="dashboard-container" phx-hook="DocumentVisibility">
      <DashboardComponents.subhead user={@user} team={@team} />
      <div class="tw-max-w-[95%] tw-mx-auto">
        <div class="lg:tw-grid tw-grid-cols-12 tw-gap-8 tw-px-[15px] tw-mt-[50px]">
          <div class="tw-col-span-3">
            <DashboardComponents.members user={@user} team={@team} team_user={@team_user} />
          </div>
          <div class="tw-col-span-7">
            <.source_list sources={@sources} source_metrics={@source_metrics} team={@team} plan={@plan} fade_in={@fade_in} />
          </div>
          <div class="tw-col-span-2">
            <DashboardComponents.integrations />
          </div>
        </div>
      </div>
    </div>
    """
  end

  attr :sources, :list, required: true
  attr :source_metrics, :map, required: true
  attr :team, Teams.Team, required: true
  attr :plan, :map, required: true
  attr :fade_in, :boolean, default: false

  def source_list(assigns) do
    ~H"""
    <div id="source-list" phx-hook="FormatTimestamps">
      <div class="tw-mb-3 tw-flex tw-justify-end">
        <.team_link team={@team} href={~p"/query"} class="btn btn-primary btn-sm">
          Run a query
        </.team_link>
        <.team_link team={@team} href={~p"/sources/new"} class="btn btn-primary btn-sm">
          New source
        </.team_link>
      </div>
      <ul class="list-group" id="source-list-group" phx-hook="LiveTooltips">
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
          <DashboardSourceComponents.source_item :for={source <- sources} source={source} plan={@plan} metrics={@source_metrics[to_string(source.token)][:metrics]} fade_in={@fade_in} team={@team} />
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
