defmodule LogflareWeb.SourceBackendsLive do
  @moduledoc false
  use LogflareWeb, :live_view
  require Logger
  alias Logflare.Backends

  def render(assigns) do
    ~H"""
    <div class="my-4">
      <%= if !@show_create_form do %>
        <button class="btn btn-primary" phx-click="toggle-create-form">Add a backend</button>
      <% else %>
        <.form :let={f} for={%{}} as={:backend_form} action="#" phx-submit="save_source_backend" class="mt-4">
          <p>Add backend</p>
          <%= select(f, :type, ["", Webhook: :webhook, Postgres: :postgres],
            phx_change: :change_create_form_type,
            class: "form-control form-control-margin"
          ) %>
          <div class="form-group mt-2">
            <%= case @create_form_type do %>
              <% "webhook" -> %>
                <%= label f, :url do %>
                  Websocket URL to send the ingested data
                <% end %>

                <%= text_input(f, :url, class: "form-control form-control-margin") %>
              <% "postgres" -> %>
                <%= label f, :url do %>
                  Postgres URL for the ingestion database
                <% end %>

                <%= text_input(f, :url, class: "form-control form-control-margin") %>
                <small class="form-text text-muted">
                  Postgres URL with the following format, for example: <code>postgresql://user:password@host:port/database</code>
                </small>
                <%= label f, :schema do %>
                  Schema where data should be store, if blank the database defaults will be used
                <% end %>

                <%= text_input(f, :schema, class: "form-control form-control-margin") %>
                <small class="form-text text-muted">
                  Schema name, for example: <code>analytics</code>
                </small>
              <% _ -> %>
                <div>Select a Backend Type</div>
            <% end %>
          </div>
          <button type="button" class="btn btn-secondary" phx-click="toggle-create-form">
            Cancel
          </button>
          <%= submit("Add", class: "btn btn-primary") %>
        </.form>
      <% end %>
      <div :if={not Enum.empty?(@source_backends)}>
        Backends:
        <ul>
          <li :for={sb <- @source_backends}>
            <%= sb.type %>
            <button class="ml-2 btn btn-danger" phx-click="remove_source_backend" phx-value-id={sb.id}>
              Remove
            </button>
            <%= case sb.type do %>
              <% :webhook -> %>
                <ul>
                  <li>url: <%= sb.config.url %></li>
                </ul>
              <% :postgres -> %>
                <ul>
                  <li>url: <%= sb.config.url %> - schema: <%= Map.get(sb.config, :schema, "") %></li>
                </ul>
            <% end %>
          </li>
        </ul>
      </div>
    </div>
    """
  end

  def mount(_params, %{"source_id" => source_id}, socket) do
    source = Logflare.Sources.get(source_id)
    source_backends = Logflare.Backends.list_source_backends(source)

    socket =
      socket
      |> assign(:source, source)
      |> assign(:source_backends, source_backends)
      |> assign(:show_create_form, false)
      |> assign(:create_form_type, nil)

    {:ok, socket, layout: {LogflareWeb.LayoutView, :inline_live}}
  end

  def handle_event("toggle-create-form", _, socket) do
    %{assigns: %{show_create_form: show_create_form}} = socket
    {:noreply, assign(socket, show_create_form: !show_create_form)}
  end

  def handle_event("save_source_backend", %{"backend_form" => params}, socket) do
    %{assigns: %{source: source}} = socket

    socket =
      case Logflare.Backends.create_source_backend(source, params["type"], params) do
        {:ok, _} ->
          assign(socket, :show_create_form, false)

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

    socket = assign(socket, :source_backends, Logflare.Backends.list_source_backends(source))

    {:noreply, socket}
  end

  def handle_event("change_create_form_type", %{"backend_form" => %{"type" => type}}, socket) do
    {:noreply, assign(socket, create_form_type: type)}
  end

  def handle_event("remove_source_backend", %{"id" => id}, %{assigns: %{source: source}} = socket) do
    Logger.debug("Removing source backend id: #{id}")
    source_backend = Backends.get_source_backend(id)
    Backends.delete_source_backend(source_backend)

    socket =
      socket
      |> put_flash(:info, "Successfully deleted backend of type #{source_backend.type}")
      |> assign(:source_backends, Backends.list_source_backends(source))

    {:noreply, socket}
  end

  defp _to_string(val) when is_list(val) do
    Enum.join(val, ", ")
  end

  defp _to_string(val), do: to_string(val)
end
