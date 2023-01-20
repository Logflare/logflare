defmodule LogflareWeb.EndpointsLive do
  @moduledoc false
  use LogflareWeb, :live_view
  require Logger
  alias Logflare.Backends
  alias Logflare.Endpoints
  alias Logflare.Users

  def render(assigns) do
    ~L"""
    <div>
      <%= live_react_component("Interfaces.EndpointsBrowserList", %{
      endpoints: @endpoints,
        selectedEndpoint: @show_endpoint}, [id: "endpoints-browser-list"]) %>

      <section>
        <%= render_action(assigns.live_action, assigns) %>
      </section>
    </div>
    """
  end

  defp render_action(:show, assigns) do
    ~L"""
    <%= live_react_component("Interfaces.ShowEndpoint", %{endpoint: @show_endpoint}, [id: "show-endpoint"]) %>
    """
  end

  defp render_action(:edit, assigns) do
    ~L"""
    <%= live_react_component("Interfaces.EndpointEditor", %{endpoint: @show_endpoint, queryResult: @query_result}, [id: "edit-endpoint"]) %>
    """
  end

  defp render_action(:new, assigns) do
    ~L"""
    <%= live_react_component("Interfaces.EndpointEditor", %{}, [id: "new-endpoint"]) %>
    """
  end

  defp render_action(_, assigns), do: ~L""

  def mount(%{}, %{"user_id" => user_id}, socket) do
    endpoints = Endpoints.list_endpoints_by(user_id: user_id)
    user = Users.get(user_id)

    {:ok,
     socket
     |> assign(:endpoints, endpoints)
     |> assign(:user_id, user_id)
     |> assign(:user, user)
     |> assign(:query_result, nil)
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

  def handle_event(
        "save-endpoint",
        %{"endpoint" => params},
        %{assigns: %{user: user, show_endpoint: show_endpoint}} = socket
      ) do
    {action, endpoint} =
      case show_endpoint do
        nil ->
          {:ok, endpoint} = Endpoints.create_query(user, params)
          {:created, endpoint}

        %_{} ->
          {:ok, endpoint} = Endpoints.update_query(show_endpoint, params)
          {:updated, endpoint}
      end

    {:noreply,
     socket
     |> put_flash(:info, "Successfully #{Atom.to_string(action)} endpoint #{endpoint.name}")
     |> push_patch(to: Routes.endpoints_path(socket, :show, endpoint))
     |> assign(:show_endpoint, endpoint)}
  end


  def handle_event("edit-endpoint", %{"endpoint_id" => id}, socket) do
    {:noreply, socket |> push_patch(to: Routes.endpoints_path(socket, :edit, id))}
  end

  def handle_event(
        "show-endpoint",
        %{"endpoint_id" => id},
        %{assigns: %{endpoints: endpoints}} = socket
      ) do
    endpoint = Enum.find(endpoints, fn e -> e.id == id end)

    {:noreply,
     assign(socket, show_endpoint: endpoint) |> push_patch(to: "/endpoints/#{endpoint.id}")}
  end


  def handle_event(
        "run-query",
        %{"query" => query},
        socket
      ) do
    result=[]
    {:noreply,
     socket
     |> put_flash(:info, "Ran query in Xs")
     |> assign(:query_result, result)}
  end

  def handle_event(
        "new-endpoint",
        _,
        socket
      ) do
    {:noreply, socket |> push_patch(to: "/endpoints/new")}
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
