defmodule LogflareWeb.EndpointsLive do
  @moduledoc false
  use LogflareWeb, :live_view
  require Logger
  alias Logflare.Backends
  alias Logflare.Endpoints

  def render(assigns) do
    ~L"""
    <div>
      <section>
      <%= for endpoint <- @endpoints do %>
        <%= live_patch endpoint.name, to: Routes.endpoints_path(@socket, :show, endpoint)  %>
      <% end %>
      </section>

      <%= render_action(assigns.live_action, assigns) %>
    </div>
    """
  end

  defp render_action(:show, assigns) do
    ~L"""
    <h3><%= @show_endpoint.name %></h3>

    <button phx-click="edit-query">Edit Query</button>
    <pre>
      <%= @show_endpoint.query %>
    </pre>
    """
  end

  defp render_action(:edit, assigns) do
    ~L"""
    <h3><%= @show_endpoint.name %></h3>

    <%= f = form_for :endpoint, "#", [phx_submit: :save_endpoint] %>
        <div class="form-group">
          <%= label f, :query %>
          <%= textarea f, :query %>
        </div>
        <button type="button" class="btn btn-secondary" phx-click="cancel-edit-query">Cancel</button>
        <%= submit "Save", class: "btn btn-primary" %>
      </form>
    """
  end

  defp render_action(_, assigns), do: ~L""

  def mount(%{}, %{"user_id" => user_id}, socket) do
    endpoints = Endpoints.list_endpoints_by(user_id: user_id)

    {:ok,
     socket
     |> assign(:endpoints, endpoints)
     |> assign(:user_id, user_id)
     |> assign(:show_endpoint, nil)}
  end

  def handle_params(params, _uri, socket) do
    endpoint_id = params["id"]

    socket =
      socket
      |> then(fn
        socket when is_binary(endpoint_id) ->
          endpoint = Endpoints.get_by(id: endpoint_id, user_id: socket.assigns.user_id)
          socket |> assign(:show_endpoint, endpoint)

        other ->
          other
      end)

    {:noreply, socket}
  end
  def handle_event("edit-query", _unsigned_params, socket) do
    socket = socket |> push_patch(to: Routes.endpoints_path(socket, :edit, socket.assigns.show_endpoint))
    {:noreply, socket}
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
      case Logflare.Backends.create_source_backend(source, params["type"], params["config"]) do
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

  def handle_event("change_create_form_type", %{"type" => type}, socket) do
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
