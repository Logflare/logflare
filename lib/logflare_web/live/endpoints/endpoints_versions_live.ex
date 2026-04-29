defmodule LogflareWeb.EndpointsVersionsLive do
  @moduledoc false
  use LogflareWeb, :live_view
  use Phoenix.Component

  import Ecto.Query
  import LogflareWeb.Utils, only: [time_ago: 1]

  alias Logflare.Endpoints
  alias Logflare.Endpoints.Query
  alias Logflare.Repo
  alias LogflareWeb.Endpoints.Components
  alias LogflareWeb.Endpoints.SnapshotModalComponent

  alias PaperTrail.Version

  @page_size 25

  @impl true
  def render(assigns) do
    ~H"""
    <.subheader>
      <:path>
        ~/<.subheader_path_link to={~p"/endpoints"} team={@team}>endpoints</.subheader_path_link>/<.subheader_path_link to={~p"/endpoints/#{@endpoint.id}"} team={@team}>
          {@endpoint.name}
        </.subheader_path_link>/versions
      </:path>
    </.subheader>

    <.versions versions={@streams.versions} current_version_id={@current_version_id} load_more?={not is_nil(@next_cursor_id)}>
      <:col :let={version} class="lg:tw-col-span-1" label="Version">
        <span :if={version.id == @current_version_id} class="tw-inline-flex tw-items-center tw-rounded-sm tw-bg-[#2155a3] tw-px-2 tw-py-1 tw-text-xs tw-font-medium tw-text-white">
          current
        </span>
        <div class="tw-w-full lg:tw-text-right tw-pt-1 tw-text-sm tw-font-medium tw-text-white ">
          {version_number(version)}
        </div>
      </:col>
      <:col :let={version} class="lg:tw-col-span-7" label="Changes">
        <div class="tw-flex tw-flex-col tw-gap-2 lg:tw-pl-2">
          <Components.change :for={change <- version_changes(version)} change={change} />
        </div>
      </:col>
      <:col :let={version} class="lg:tw-col-span-2" label="Author">
        <span class="tw-text-sm tw-font-medium tw-text-zinc-300 lg:tw-whitespace-nowrap">{version.origin || "unknown"}</span>
      </:col>
      <:col :let={version} class="lg:tw-col-span-2" label="Updated">
        <div class="tw-flex tw-flex-col tw-gap-0.5">
          <span class="tw-text-sm tw-font-medium tw-text-zinc-200 tw-tabular-nums">
            {Calendar.strftime(version.inserted_at, "%Y-%m-%d %H:%M:%S UTC")}
          </span>
          <div class="tw-flex tw-items-center tw-gap-2">
            <span class="tw-text-xs tw-text-zinc-500 ">
              {time_ago(version.inserted_at)}
            </span>
          </div>
        </div>
      </:col>
    </.versions>

    <.live_component
      :if={@selected_version}
      module={LogflareWeb.ModalComponent}
      id="endpoint-version-snapshot-modal"
      title={snapshot_to_endpoint(@selected_version).description}
      return_to={
        LogflareWeb.Utils.with_team_param(
          ~p"/endpoints/#{@endpoint.id}/versions",
          @team
        )
      }
      component={SnapshotModalComponent}
      is_template?={false}
      opts={
        %{
          id: "endpoint-version-snapshot-content",
          version: @selected_version,
          snapshot: snapshot_to_endpoint(@selected_version)
        }
      }
    />
    """
  end

  attr :versions, :any, required: true
  attr :current_version_id, :integer, default: nil
  attr :load_more?, :boolean, default: false

  slot :col do
    attr :label, :string
    attr :class, :string
  end

  def versions(assigns) do
    ~H"""
    <section class="mx-auto container pt-3 tw-flex tw-flex-col tw-gap-5">
      <div class="tw-hidden lg:tw-grid tw-grid-cols-12 tw-px-4 tw-text-left tw-text-sm tw-font-semibold tw-text-zinc-400">
        <div :for={col <- @col} class={["first:tw-text-center tw-px-4 tw-py-2", col[:class]]}>{col.label}</div>
      </div>

      <div class="tw-flex tw-flex-col tw-gap-3" id="endpoint-versions" phx-update="stream">
        <div id="versions-empty" class="tw-hidden only:tw-flex tw-min-h-[12rem] tw-w-full tw-items-center tw-justify-center tw-rounded-lg tw-border tw-border-zinc-800 tw-bg-dashboard-grey tw-p-4 tw-text-center">
          <p class="tw-mb-0 tw-text-zinc-400">
            No versions recorded for this endpoint.
          </p>
        </div>
        <a
          :for={{dom_id, version} <- @versions}
          id={dom_id}
          phx-click="show-version"
          phx-value-version-number={version_number(version)}
          href="#"
          class="tw-block tw-rounded tw-border tw-border-zinc-900 tw-bg-dashboard-grey tw-no-underline hover:tw-border-zinc-700 hover:tw-bg-[#232323] focus:tw-outline-none focus:tw-ring-2 focus:tw-ring-[#2155a3] lg:tw-grid tw-grid-cols-12"
        >
          <div :for={col <- @col} class={["tw-px-4 tw-py-2", col[:class]]}>
            <div class="tw-mb-2 lg:tw-hidden tw-text-sm tw-font-semibold tw-text-zinc-400">
              {col.label}
            </div>
            <div class="tw-flex tw-gap-2 lg:tw-pt-1">
              {render_slot(col, version)}
            </div>
          </div>
        </a>
      </div>

      <div :if={@load_more?} class="tw-flex tw-justify-center">
        <button type="button" phx-click="load-more" class="btn btn-primary">
          Load more
        </button>
      </div>
    </section>
    """
  end

  @impl true
  def mount(%{"id" => endpoint_id} = params, _session, socket) do
    user = socket.assigns.team_user || socket.assigns.user

    socket =
      case Endpoints.get_endpoint_query_by_user_access(user, endpoint_id) do
        nil ->
          redirect(socket, to: ~p"/endpoints")

        endpoint ->
          {versions, next_cursor_id} = fetch_page(endpoint.id)

          socket
          |> assign(:endpoint, endpoint)
          |> assign(:current_version_id, current_version_id(versions))
          |> assign(:next_cursor_id, next_cursor_id)
          |> assign(:selected_version, nil)
          |> maybe_assign_team_context(params, endpoint)
          |> stream(:versions, versions, reset: true)
      end

    {:ok, socket}
  end

  @impl true
  def handle_params(%{"version_number" => version_number}, _uri, socket) do
    endpoint = socket.assigns.endpoint

    socket =
      with {version_number, ""} <- Integer.parse(version_number),
           selected_version when is_struct(selected_version) <-
             Endpoints.get_endpoint_query_version_by_version_number(endpoint.id, version_number) do
        socket
        |> assign(:selected_version, selected_version)
      else
        _ ->
          socket
      end

    {:noreply, socket}
  end

  def handle_params(_params, _uri, socket), do: {:noreply, socket}

  @impl true
  def handle_event("load-more", _params, socket) do
    case socket.assigns.next_cursor_id do
      nil ->
        {:noreply, socket}

      after_version_id ->
        {versions, next_cursor_id} = fetch_page(socket.assigns.endpoint.id, after_version_id)

        {:noreply,
         socket
         |> assign(:next_cursor_id, next_cursor_id)
         |> stream(:versions, versions)}
    end
  end

  @impl true
  def handle_event("show-version", %{"version-number" => version_number}, socket) do
    endpoint = socket.assigns.endpoint

    {:noreply,
     push_patch(socket,
       to:
         LogflareWeb.Utils.with_team_param(
           ~p"/endpoints/#{endpoint.id}/versions?#{%{version_number: version_number}}",
           socket.assigns[:team]
         )
     )}
  end

  @spec fetch_page(integer(), integer() | nil) :: {[Version.t()], integer() | nil}
  defp fetch_page(endpoint_id, after_version_id \\ nil) do
    fetched_versions = fetch_versions(endpoint_id, after_version_id)

    {Enum.take(fetched_versions, @page_size), next_cursor_id(fetched_versions)}
  end

  @spec fetch_versions(integer(), integer() | nil) :: [Version.t()]
  defp fetch_versions(endpoint_id, after_version_id) do
    Version
    |> where([version], version.item_type == "Query" and version.item_id == ^endpoint_id)
    |> maybe_filter_after_version(after_version_id)
    |> order_by([version], desc: version.id)
    |> limit(^(@page_size + 1))
    |> Repo.all()
  end

  @spec maybe_filter_after_version(Ecto.Queryable.t(), integer() | nil) :: Ecto.Query.t()
  defp maybe_filter_after_version(query, nil), do: query

  defp maybe_filter_after_version(query, after_version_id) do
    where(query, [version], version.id < ^after_version_id)
  end

  @spec current_version_id([Version.t()]) :: integer() | nil
  defp current_version_id([%Version{id: version_id} | _]), do: version_id
  defp current_version_id(_versions), do: nil

  @spec next_cursor_id([Version.t()]) :: integer() | nil
  defp next_cursor_id(versions) do
    if length(versions) > @page_size do
      case Enum.at(versions, @page_size - 1) do
        %Version{id: version_id} -> version_id
        _ -> nil
      end
    else
      nil
    end
  end

  @spec version_changes(Version.t()) :: [map()]
  defp version_changes(version) do
    version
    |> Map.get(:item_changes)
    |> Enum.map(fn
      {"query" = field, _value} ->
        %{
          field: field,
          query_diff: version_query_diff(version)
        }

      {field, value} ->
        %{
          field: field,
          value: value
        }
    end)
  end

  defp version_query_diff(%Version{meta: meta}) when is_map(meta) do
    case Map.get(meta, "query_diff") do
      query_diff when is_list(query_diff) -> Enum.map(query_diff, &normalize_query_diff_segment/1)
      _ -> []
    end
  end

  defp version_query_diff(_version), do: []

  defp query_diff_class("eq"), do: "tw-text-zinc-500"
  defp query_diff_class("del"), do: "tw-bg-red-950/40 tw-text-red-300 tw-line-through"
  defp query_diff_class("ins"), do: "tw-bg-emerald-950/40 tw-font-bold tw-text-emerald-400 px-1"
  defp query_diff_class(_type), do: "tw-text-zinc-500"

  defp normalize_query_diff_segment(%{"type" => type, "value" => value}),
    do: %{value: value, class: query_diff_class(type)}

  defp version_number(%Version{meta: %{"version_number" => version_number}}), do: version_number
  defp version_number(_version), do: nil

  @spec snapshot_to_endpoint(Version.t()) :: Query.t()
  defp snapshot_to_endpoint(version) do
    snapshot = Map.get(version.meta, "endpoint_snapshot", %{})

    %Query{}
    |> Ecto.Changeset.cast(snapshot, Endpoints.version_snapshot_fields())
    |> Ecto.Changeset.apply_changes()
  end

  defp maybe_assign_team_context(socket, %{"t" => _team_id}, _endpoint), do: socket

  defp maybe_assign_team_context(socket, _params, endpoint) do
    LogflareWeb.AuthLive.assign_context_by_resource(socket, endpoint, socket.assigns.user.email)
  end
end
