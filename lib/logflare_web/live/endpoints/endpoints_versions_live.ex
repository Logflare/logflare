defmodule LogflareWeb.EndpointsVersionsLive do
  @moduledoc false
  use LogflareWeb, :live_view
  use Phoenix.Component

  import Ecto.Query
  import LogflareWeb.Utils, only: [time_ago: 1]

  alias Logflare.Endpoints
  alias Logflare.Repo
  alias LogflareWeb.ErrorsLive.InvalidResourceError
  alias PaperTrail.Version

  @type history_change :: %{
          required(:field) => String.t(),
          optional(:value) => String.t(),
          optional(:query_diff) => [query_diff_segment()]
        }

  @type query_diff_segment :: %{
          required(:value) => String.t(),
          required(:class) => String.t()
        }

  @type query_diff_meta_segment :: %{
          required(:type) => String.t(),
          required(:value) => String.t()
        }

  @type history_entry :: %{
          required(:version_number) => integer() | nil,
          required(:updated_at) => DateTime.t() | NaiveDateTime.t(),
          required(:author) => String.t(),
          required(:changes) => [history_change()]
        }

  @impl true
  def render(assigns) do
    ~H"""
    <.subheader>
      <:path>
        ~/<.subheader_path_link live_patch to={~p"/endpoints"} team={@team}>endpoints</.subheader_path_link>/<.subheader_path_link live_patch to={~p"/endpoints/#{@endpoint.id}"}>
          {@endpoint.name}
        </.subheader_path_link>/versions
      </:path>
    </.subheader>

    <.history entries={@entries} />
    """
  end

  attr :entries, :list, default: []

  def history(%{entries: []} = assigns) do
    ~H"""
    <section :if={@entries == []} class="mx-auto container tw-flex  tw-items-center tw-justify-center pt-3">
      <div class="tw-flex tw-min-h-[12rem] tw-w-full tw-items-center tw-justify-center tw-rounded-lg tw-border tw-border-zinc-800 tw-bg-zinc-900 tw-p-4 tw-text-center">
        <p class="tw-mb-0 tw-text-zinc-400">
          No versions recorded for this endpoint.
        </p>
      </div>
    </section>
    """
  end

  def history(assigns) do
    ~H"""
    <section class="mx-auto container pt-3 tw-flex tw-flex-col tw-gap-5">
      <div class="tw-flex tw-flex-col tw-gap-3">
        <div class="tw-hidden lg:tw-grid tw-grid-cols-12 tw-px-4 tw-text-left tw-text-xs tw-font-medium tw-text-zinc-500">
          <div class="tw-col-span-1 tw-py-2 tw-text-center">Version</div>
          <div class="tw-col-span-7 tw-px-4 tw-py-2 tw-pl-6">Changes</div>
          <div class="tw-col-span-2 tw-px-4 tw-py-2">Author</div>
          <div class="tw-col-span-2 tw-px-4 tw-py-2">Updated</div>
        </div>

        <%= for {entry, idx} <- Enum.with_index(@entries) do %>
          <div class="tw-rounded tw-bg-[#1d1d1d] lg:tw-grid tw-grid-cols-12">
            <div class="tw-px-4 tw-py-4 lg:tw-col-span-1 lg:tw-border-r lg:tw-border-black/20">
              <div class="tw-mb-2 lg:tw-hidden tw-text-xs tw-font-medium tw-uppercase tw-tracking-wide tw-text-zinc-500">
                Version
              </div>
              <div class="tw-flex tw-items-center tw-justify-end tw-gap-2">
                <span :if={idx == 0} class="badge badge-success">current</span>
                <div class="tw-text-right tw-text-sm tw-font-medium tw-text-white tw-tabular-nums">
                  {format_version_number(entry.version_number)}
                </div>
              </div>
            </div>

            <div class="tw-px-4 tw-py-4 lg:tw-col-span-7 lg:tw-border-r lg:tw-border-black/20">
              <div class="tw-mb-2 lg:tw-hidden tw-text-xs tw-font-medium tw-uppercase tw-tracking-wide tw-text-zinc-500">
                Changes
              </div>
              <div class="tw-flex tw-flex-col tw-gap-2 lg:tw-pl-2">
                <.change :for={change <- entry.changes} change={change} />
              </div>
            </div>

            <div class="tw-px-2 tw-py-4 lg:tw-col-span-2">
              <div class="tw-mb-2 lg:tw-hidden tw-text-xs tw-font-medium tw-uppercase tw-tracking-wide tw-text-zinc-500">
                Author
              </div>
              <div class="tw-text-sm tw-text-zinc-500 lg:tw-whitespace-nowrap">{entry.author}</div>
            </div>

            <div class="tw-px-4 tw-py-4 lg:tw-col-span-2 lg:tw-border-l lg:tw-border-black/20">
              <div class="tw-mb-2 lg:tw-hidden tw-text-xs tw-font-medium tw-uppercase tw-tracking-wide tw-text-zinc-500">
                Updated
              </div>
              <div class="tw-flex tw-flex-col tw-gap-0.5">
                <span class="tw-text-sm tw-font-medium tw-text-white tw-tabular-nums">
                  {Calendar.strftime(entry.updated_at, "%Y-%m-%d %H:%M:%S UTC")}
                </span>
                <div class="tw-flex tw-items-center tw-gap-2">
                  <span class="tw-text-xs tw-text-zinc-500">
                    {time_ago(entry.updated_at)}
                  </span>
                </div>
              </div>
            </div>
          </div>
        <% end %>
      </div>
    </section>
    """
  end

  attr :change, :map, required: true

  def change(%{change: %{query_diff: _}} = assigns) do
    ~H"""
    <div class="tw-grid tw-grid-cols-[12rem_minmax(0,1fr)] tw-items-start tw-gap-2 tw-rounded-sm tw-px-2 tw-py-1 tw-text-sm tw-text-zinc-300">
      <span class="tw-pr-2 tw-text-zinc-400 tw-font-sans">{@change.field}:</span>
      <div class="tw-min-w-0 tw-rounded tw-bg-[#1e1e1e] tw-py-1 tw-font-mono tw-text-xs [&_pre]:tw-m-0 [&_pre]:tw-whitespace-pre-wrap [&_pre]:tw-break-words [&_pre]:tw-overflow-x-visible">
        <pre><%= for segment <- @change.query_diff do %><span class={segment.class}>{segment.value}</span><% end %></pre>
      </div>
    </div>
    """
  end

  def change(assigns) do
    ~H"""
    <div class="tw-grid tw-grid-cols-[12rem_minmax(0,1fr)] tw-items-baseline tw-gap-2 tw-rounded-sm tw-px-2 tw-py-1 tw-text-sm tw-text-zinc-300">
      <span class="tw-pr-2 tw-text-zinc-400 tw-font-sans">{@change.field}:</span>
      <span class="tw-min-w-0 tw-font-mono tw-text-zinc-300 tw-normal-case tw-break-words">
        {@change.value}
      </span>
    </div>
    """
  end

  @impl true
  def mount(%{}, _session, socket) do
    {:ok,
     socket
     |> assign(:endpoint, nil)
     |> assign(:entries, [])}
  end

  @impl true
  def handle_params(%{"id" => endpoint_id} = params, _uri, socket) do
    user = socket.assigns.team_user || socket.assigns.user
    endpoint = Endpoints.get_endpoint_query_by_user_access(user, endpoint_id)

    if is_nil(endpoint), do: raise(InvalidResourceError)

    socket =
      socket
      |> assign(:endpoint, endpoint)
      |> assign(:entries, history_entries(endpoint))
      |> maybe_assign_team_context(params, endpoint)

    {:noreply, socket}
  end

  @spec history_entries(Endpoints.Query.t() | nil) :: [history_entry()]
  defp history_entries(nil), do: []

  defp history_entries(endpoint) do
    versions = fetch_versions(endpoint.id)

    {entries, _state} =
      Enum.map_reduce(versions, %{}, fn version, previous_state ->
        current_state = version_state(version, previous_state)

        entry =
          %{
            version_number: version_number(version),
            updated_at: version.inserted_at,
            author: version.origin || "unknown",
            changes: build_changes(changed_fields(version), previous_state, current_state)
          }

        {entry, current_state}
      end)

    entries
    |> Enum.filter(&(not Enum.empty?(&1.changes)))
    |> Enum.reverse()
  end

  @spec fetch_versions(integer()) :: [Version.t()]
  defp fetch_versions(endpoint_id) do
    from(version in Version,
      where: version.item_type == "Query" and version.item_id == ^endpoint_id,
      order_by: [asc: version.inserted_at, asc: version.id]
    )
    |> Repo.all()
  end

  @spec normalize_changes(map() | nil) :: map()
  defp normalize_changes(nil), do: %{}

  defp normalize_changes(item_changes) when is_map(item_changes) do
    Map.new(item_changes, fn {key, value} -> {to_string(key), value} end)
  end

  @spec version_state(Version.t(), map()) :: map()
  defp version_state(version, previous_state) do
    case version_snapshot(version) do
      %{} = snapshot ->
        maybe_put_version_query_diff(snapshot, version)

      nil ->
        previous_state
        |> Map.merge(normalize_changes(version.item_changes))
        |> maybe_put_version_query_diff(version)
    end
  end

  @spec version_snapshot(Version.t()) :: map() | nil
  defp version_snapshot(%Version{meta: meta}) when is_map(meta) do
    case Map.get(meta, "endpoint_snapshot", Map.get(meta, :endpoint_snapshot)) do
      snapshot when is_map(snapshot) -> normalize_changes(snapshot)
      _ -> nil
    end
  end

  defp version_snapshot(_version), do: nil

  @spec version_number(Version.t()) :: integer() | nil
  defp version_number(%Version{meta: meta}) when is_map(meta) do
    case Map.get(meta, "version_number", Map.get(meta, :version_number)) do
      version_number when is_integer(version_number) -> version_number
      version_number when is_binary(version_number) -> String.to_integer(version_number)
      _ -> nil
    end
  end

  defp version_number(_version), do: nil

  @spec changed_fields(Version.t()) :: [String.t()]
  defp changed_fields(%Version{item_changes: item_changes}) do
    item_changes
    |> normalize_changes()
    |> Map.keys()
    |> Enum.sort()
  end

  @spec build_changes([String.t()], map(), map()) :: [history_change()]
  defp build_changes(fields, previous_state, current_state) do
    fields
    |> Enum.filter(fn field ->
      Map.get(previous_state, field) != Map.get(current_state, field)
    end)
    |> Enum.map(fn field ->
      current_value = Map.get(current_state, field)

      case field do
        "query" ->
          %{
            field: humanize_field(field),
            query_diff:
              version_query_diff(
                Map.get(current_state, "__version_query_diff__"),
                Map.get(previous_state, field),
                current_value
              )
          }

        _ ->
          %{
            field: humanize_field(field),
            value: format_field_value(field, current_value)
          }
      end
    end)
  end

  @spec humanize_field(String.t()) :: String.t()
  defp humanize_field("query"), do: "Query"
  defp humanize_field("enable_auth"), do: "Enable authentication"
  defp humanize_field("cache_duration_seconds"), do: "Cache TTL"
  defp humanize_field("proactive_requerying_seconds"), do: "Requery interval"
  defp humanize_field("max_limit"), do: "Max rows"
  defp humanize_field("redact_pii"), do: "Redact PII"
  defp humanize_field("enable_dynamic_reservation"), do: "Dynamic reservation"
  defp humanize_field("backend_id"), do: "Backend"

  defp humanize_field(field) do
    field
    |> String.split("_")
    |> Enum.map_join(" ", &String.capitalize/1)
  end

  @spec format_field_value(String.t(), term()) :: String.t()
  defp format_field_value(_field, nil), do: "—"
  defp format_field_value("cache_duration_seconds", value) when is_integer(value), do: "#{value}s"

  defp format_field_value("proactive_requerying_seconds", value) when is_integer(value),
    do: "#{value}s"

  defp format_field_value(_field, value) when is_boolean(value), do: boolean_label(value)
  defp format_field_value(_field, value) when is_atom(value), do: Atom.to_string(value)
  defp format_field_value(_field, value) when is_binary(value), do: value
  defp format_field_value(_field, value) when is_integer(value), do: Integer.to_string(value)
  defp format_field_value(_field, value), do: inspect(value)

  @spec boolean_label(boolean()) :: String.t()
  defp boolean_label(true), do: "enabled"
  defp boolean_label(false), do: "disabled"

  @spec format_version_number(integer() | nil) :: String.t()
  defp format_version_number(version_number) when is_integer(version_number),
    do: Integer.to_string(version_number)

  defp format_version_number(nil), do: "—"

  @spec version_query_diff([query_diff_meta_segment()] | nil, term(), term()) ::
          [query_diff_segment()]
  defp version_query_diff(query_diff, _previous_query, _current_query) when is_list(query_diff) do
    Enum.map(query_diff, &normalize_query_diff_segment/1)
  end

  defp version_query_diff(nil, _previous_query, _current_query), do: []

  @spec query_diff_class(String.t()) :: String.t()
  defp query_diff_class("eq"), do: "tw-text-zinc-500"
  defp query_diff_class("del"), do: "tw-bg-red-950/40 tw-text-red-300 tw-line-through"
  defp query_diff_class("ins"), do: "tw-bg-emerald-950/40 tw-font-bold tw-text-emerald-400 px-1"
  defp query_diff_class(_type), do: "tw-text-zinc-500"

  @spec normalize_query_diff_segment(query_diff_meta_segment() | map()) :: query_diff_segment()
  defp normalize_query_diff_segment(%{"type" => type, "value" => value}),
    do: %{value: value, class: query_diff_class(type)}

  defp normalize_query_diff_segment(%{type: type, value: value}),
    do: %{value: value, class: query_diff_class(type)}

  @spec maybe_put_version_query_diff(map(), Version.t()) :: map()
  defp maybe_put_version_query_diff(state, %Version{meta: meta}) when is_map(meta) do
    case Map.get(meta, "query_diff", Map.get(meta, :query_diff)) do
      query_diff when is_list(query_diff) -> Map.put(state, "__version_query_diff__", query_diff)
      _ -> Map.delete(state, "__version_query_diff__")
    end
  end

  defp maybe_put_version_query_diff(state, _version),
    do: Map.delete(state, "__version_query_diff__")

  defp maybe_assign_team_context(socket, %{"t" => _team_id}, _endpoint), do: socket

  defp maybe_assign_team_context(socket, _params, endpoint) do
    LogflareWeb.AuthLive.assign_context_by_resource(socket, endpoint, socket.assigns.user.email)
  end
end
