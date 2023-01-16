defmodule LogflareWeb.SourceBackendsLive do
  @moduledoc false
  use LogflareWeb, :live_view
  require Logger
  alias Logflare.Backends

  def render(assigns) do
    ~L"""
    <div class="mt-4">
      <%= if !@show_create_form do %>
        <button class="btn btn-primary" phx-click="toggle-create-form">Add a backend</button>
      <% end %>
      <%= if @show_create_form do %>
      <%= f = form_for :source_backend, "#", [ phx_change: :change_create_source_backend, phx_submit: :save_source_backend, class: "mt-4"] %>
        Add <%= select f, :type, [:webhook, :google_analytics] %> backend
        <div class="form-group mt-2">
          <%= for f_config <- inputs_for(f, :config) do %>
            <%= case @create_form_type do %>
              <% "webhook" -> %>
                <%= label f_config, :url %>
                <%= text_input f_config, :url %>
              <% "google_analytics" -> %>
                <%= label f_config, :event_name_paths %>
                <%= text_input f_config, :event_name_paths %>
                <small class="form-text text-muted">
                  LQL paths for the event name. Comma separate the paths to send multiple GA events with different names. E.g. <code>metadata.my.nested.key</code>
                </small>

                <%= label f_config, :client_id_path, "Client ID path" %>
                <%= text_input f_config, :client_id_path %>
                <small class="form-text text-muted">
                  LQL path to a client id. Only one path allowed.
                </small>

                <%= label f_config, :api_secret, "API secret"  %>
                <%= text_input f_config, :api_secret%>
                <small class="form-text text-muted">
                  For authenticating with the GA4 API
                </small>

                <%= label f_config, :measurement_id, "Measurement ID" %>
                <%= text_input f_config, :measurement_id %>
                <small class="form-text text-muted">
                  For your GA4 project. E.g. G-13E2DF
                </small>

            <% end %>
          <% end %>
        </div>
        <button type="button" class="btn btn-secondary" phx-click="toggle-create-form">Cancel</button>
        <%= submit "Add", class: "btn btn-primary" %>
      </form>
      <% end %>

      <%= if length(@source_backends) > 0 do %>
      <div>
        Backends:
        <ul>
          <%= for sb <- @source_backends do %>
            <li>
                <%= sb.type %> <button class="ml-2 btn btn-danger" phx-click="remove_source_backend" phx-value-id="<%= sb.id %>">Remove</button>
                <%= case sb.type do %>
                <% :webhook -> %>
                  <ul>
                    <li>url: <%= sb.config.url %></li>
                  </ul>
                <% :google_analytics -> %>
                  <ul>
                    <li>Measurement ID: <%= sb.config.measurement_id %></li>
                    <li>API Secret: <%= sb.config.api_secret |> String.slice(0, 3) %>******</li>
                    <li>Client ID Path: <%= sb.config.client_id_path %></li>
                    <li>Event Name Paths: <%= sb.config.event_name_paths %></li>
                  </ul>
              <% end %>
            </li>
          <% end %>
        </ul>
      </div>
      <% else %>
        <p>No source backends yet.</p>
      <% end %>

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
      |> assign(:create_form_type, "webhook")
      |> assign(
        :create_changeset,
        gen_create_changeset(source_id, "webhook")
      )

    {:ok, socket, layout: {LogflareWeb.LayoutView, "inline_live.html"}}
  end

  def handle_event(
        "toggle-create-form",
        _,
        %{assigns: %{show_create_form: show_create_form}} = socket
      ) do
    {:noreply, assign(socket, show_create_form: !show_create_form)}
  end

  def handle_event(
        "save_source_backend",
        %{"source_backend" => params},
        %{assigns: %{source: source}} = socket
      ) do
    socket =
      case Logflare.Backends.create_source_backend(
             source,
             params["type"] |> IO.inspect(),
             params["config"]
           ) do
        {:ok, _} ->
          socket
          |> assign(:show_create_form, false)

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

          socket
          |> put_flash(:error, "Encountered error when adding backend:\n#{message}")
      end

    socket =
      socket
      |> assign(:source_backends, Logflare.Backends.list_source_backends(source))

    {:noreply, socket}
  end

  def handle_event(
        "change_create_source_backend",
        %{
          "_target" => ["source_backend", "type"],
          "source_backend" => %{"type" => type}
        },
        socket
      ) do
    socket =
      socket
      |> assign(
        :create_form_type,
        type
      )
      |> assign(
        :create_changeset,
        gen_create_changeset(socket.assigns.source.id, type)
      )

    {:noreply, socket}
  end

  def handle_event("change_create_source_backend", _params, socket), do: {:noreply, socket}

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

  defp gen_create_changeset(source_id, type) do
    Logflare.Backends.SourceBackend.changeset(%Logflare.Backends.SourceBackend{}, %{
      source_id: source_id,
      type: type,
      config: %{}
    })
  end
end
