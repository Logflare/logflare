defmodule LogflareWeb.SourceBackendsLive do
  @moduledoc false
  use LogflareWeb, :live_view
  require Logger
  alias Logflare.Backends

  def render(assigns) do
    ~H"""
    <div class="my-4">
      <div :if={not Enum.empty?(@backends)}>
        <h5>Backends</h5>
        <small class="badge badge-pill badge-success">connected: <%= Enum.count(@attached_backend_ids) %></small>
        <.form :let={f} as={:source} for={%{}} action="#" phx-submit="save">
          <% grouped = Enum.group_by(@backends, & &1.type) %>
          <%= for type <- [:bigquery, :postgres, :webhook, :datadog],
             backends = Map.get(grouped, type, []) do %>
            <div class="form-group">
              <strong>
                <%= case type do
                  :bigquery -> "BigQuery"
                  :postgres -> "PostgreSQL"
                  :webhook -> "Webhook"
                  :datadog -> "Datadog"
                end %>
              </strong>

              <div :if={type == :bigquery} class="form-row custom-control custom-switch">
                <%= text_input(f, :backends, type: "checkbox", class: "custom-control-input", id: "backends-default", disabled: true, checked: true) %>
                <%= label(f, :backends, "Logflare-managed BigQuery", class: "custom-control-label", for: "backends-default") %>
              </div>
              <div :for={backend <- backends} class="form-row custom-control custom-switch">
                <%= text_input(f, :backends, type: "checkbox", class: "custom-control-input", id: "backends-#{backend.id}", name: "source[backends][]", checked: backend.id in @attached_backend_ids, value: backend.id) %>
                <%= label(f, :backends, backend.name, class: "custom-control-label", for: "backends-#{backend.id}") %>
              </div>
              <div :if={Enum.empty?(backends) and type !== :bigquery}>
                <small class="text-muted">No backend created yet!</small>
              </div>
            </div>
          <% end %>

          <%= submit("Save", class: "btn btn-primary form-button") %>
        </.form>
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

  def handle_event(
        "save",
        %{"source" => %{"backends" => ids}},
        %{assigns: %{backends: backends, source: source}} = socket
      ) do
    backend_ids = for id <- ids, {val, _rem} = Integer.parse(id), do: val

    backends = for backend <- backends, backend.id in backend_ids, do: backend

    socket =
      case Backends.update_source_backends(source, backends) do
        {:ok, _source} ->
          socket
          |> refresh_data()
          |> put_flash(:info, "Successfully updated attached backends!")

        {:error, changeset} ->
          # TODO: move this to a helper function
          message =
            Ecto.Changeset.traverse_errors(changeset, fn {msg, opts} ->
              Enum.reduce(opts, msg, fn {key, value}, acc ->
                String.replace(acc, "%{#{key}}", _to_string(value))
              end)
            end)
            |> Enum.reduce("", fn {k, v}, acc ->
              joined_errors = Enum.join(v, ";\n")
              "#{acc} #{k}: #{joined_errors}"
            end)

          put_flash(socket, :error, "Encountered error when adding backend:\n#{message}")
      end

    {:noreply, socket}
  end

  defp refresh_data(%{assigns: %{source_id: source_id}} = socket) do
    source =
      Logflare.Sources.get(source_id)
      |> Logflare.Sources.preload_backends()

    backends = Logflare.Backends.list_backends_by_user_id(source.user_id)
    attached_backend_ids = for b <- source.backends, do: b.id

    socket
    |> assign(:source, source)
    |> assign(:backends, backends)
    |> assign(:attached_backend_ids, attached_backend_ids)
  end

  defp _to_string(val) when is_list(val) do
    Enum.join(val, ", ")
  end

  defp _to_string(val), do: to_string(val)
end
