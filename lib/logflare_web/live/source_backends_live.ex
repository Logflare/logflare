defmodule LogflareWeb.SourceBackendsLive do
  @moduledoc false
  use LogflareWeb, :live_view

  import LogflareWeb.Utils, only: [stringify_changeset_errors: 1]

  alias Logflare.Backends
  alias Logflare.Sources

  def render(assigns) do
    ~H"""
    <div class="my-4">
      <h5>Backend Configuration</h5>

      <div class="form-group mt-3">
        <strong>System Backend</strong>
        <p class="text-muted small">
          The default ingestion backend managed by Logflare (BigQuery or PostgreSQL depending on deployment).
          Disable this if you only want to route events to your custom backends.
        </p>
        <div class="form-row custom-control custom-switch">
          <input
            type="checkbox"
            class="custom-control-input"
            id="system-backend-enabled"
            checked={@system_backend_enabled?}
            phx-click="toggle_system_backend"
          />
          <label class="custom-control-label" for="system-backend-enabled">
            Enable system backend ingestion
            <span class={["badge badge-pill", if(@system_backend_enabled?, do: "badge-success", else: "badge-secondary")]}>
              {if @system_backend_enabled?, do: "enabled", else: "disabled"}
            </span>
          </label>
        </div>
      </div>

      <div :if={not Enum.empty?(@backends)} class="mt-3">
        <strong>Custom Backends</strong>
        <small class="badge badge-pill badge-success ml-1">connected: {Enum.count(@attached_backend_ids)}</small>
        <.form :let={f} as={:source} for={%{}} action="#" phx-submit="save">
          <% grouped = Enum.group_by(@backends, & &1.type) %>
          <%= for type <- [:bigquery, :postgres, :clickhouse, :syslog, :webhook, :datadog, :sentry],
             backends = Map.get(grouped, type, []),
             not Enum.empty?(backends) do %>
            <div class="form-group mt-2">
              <em>
                {case type do
                  :bigquery -> "BigQuery"
                  :postgres -> "PostgreSQL"
                  :clickhouse -> "ClickHouse"
                  :syslog -> "Syslog"
                  :webhook -> "Webhook"
                  :datadog -> "Datadog"
                  :sentry -> "Sentry"
                end}
              </em>
              <div :for={backend <- backends} class="form-row custom-control custom-switch">
                {text_input(f, :backends,
                  type: "checkbox",
                  class: "custom-control-input",
                  id: "backends-#{backend.id}",
                  name: "source[backends][]",
                  checked: backend.id in @attached_backend_ids,
                  value: backend.id
                )}
                {label(f, :backends, backend.name,
                  class: "custom-control-label",
                  for: "backends-#{backend.id}"
                )}
              </div>
            </div>
          <% end %>

          {submit("Save custom backends", class: "btn btn-primary form-button mt-2")}
        </.form>
      </div>

      <div :if={Enum.empty?(@backends)} class="mt-2">
        <small class="text-muted">No custom backends created yet.</small>
      </div>
    </div>
    """
  end

  def mount(_params, %{"source_id" => source_id}, socket) do
    socket =
      socket
      |> assign(:source_id, source_id)
      |> refresh_data()

    {:ok, socket, layout: {LogflareWeb.LayoutView, :inline_live}}
  end

  def handle_event("toggle_system_backend", _params, %{assigns: %{source: source}} = socket) do
    new_value = not source.system_backend_enabled?

    socket =
      case Sources.update_source_by_user(source, %{"system_backend_enabled?" => new_value}) do
        {:ok, _updated} ->
          socket
          |> refresh_data()
          |> put_flash(
            :info,
            if(new_value,
              do: "System backend ingestion enabled.",
              else: "System backend ingestion disabled."
            )
          )

        {:error, changeset} ->
          message = stringify_changeset_errors(changeset)
          put_flash(socket, :error, "Error updating system backend: #{message}")
      end

    {:noreply, socket}
  end

  def handle_event(
        "save",
        %{"source" => %{"backends" => ids}},
        %{assigns: %{backends: backends, source: source}} = socket
      ) do
    backend_ids = for id <- ids, {val, _rem} = Integer.parse(id), do: val
    selected_backends = for backend <- backends, backend.id in backend_ids, do: backend

    socket =
      case Backends.update_source_backends(source, selected_backends) do
        {:ok, _source} ->
          socket
          |> refresh_data()
          |> put_flash(:info, "Successfully updated attached backends!")

        {:error, changeset} ->
          message = stringify_changeset_errors(changeset)
          put_flash(socket, :error, "Encountered error when adding backend:\n#{message}")
      end

    {:noreply, socket}
  end

  def handle_event("save", %{"source" => %{}}, socket) do
    socket =
      case Backends.update_source_backends(socket.assigns.source, []) do
        {:ok, _source} ->
          socket
          |> refresh_data()
          |> put_flash(:info, "Successfully updated attached backends!")

        {:error, changeset} ->
          message = stringify_changeset_errors(changeset)
          put_flash(socket, :error, "Encountered error when updating backends:\n#{message}")
      end

    {:noreply, socket}
  end

  defp refresh_data(%{assigns: %{source_id: source_id}} = socket) do
    source =
      Sources.get(source_id)
      |> Sources.preload_backends()

    backends = Backends.list_backends_by_user_id(source.user_id)
    attached_backend_ids = for b <- source.backends, do: b.id

    socket
    |> assign(:source, source)
    |> assign(:system_backend_enabled?, source.system_backend_enabled?)
    |> assign(:backends, backends)
    |> assign(:attached_backend_ids, attached_backend_ids)
  end
end
