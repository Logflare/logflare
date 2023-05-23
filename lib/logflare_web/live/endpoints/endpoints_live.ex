defmodule LogflareWeb.EndpointsLive do
  @moduledoc false
  use LogflareWeb, :live_view
  require Logger
  alias Logflare.Endpoints
  alias Logflare.Endpoints.Query
  alias Logflare.Users
  alias LogflareWeb.Utils
  use Phoenix.Component
  embed_templates "actions/*", suffix: "_action"
  embed_templates "components/*"


  def render(%{allow_access: false} = assigns), do: closed_beta_action(assigns)

  def render(%{live: :index} = assigns), do: list_action(assigns)
  def render(%{live: :show} = assigns), do: show_action(assigns)


  defp render_action(:show, %{show_endpoint: nil} = assigns) do
    ~H"""
    <%= live_react_component("Interfaces.EndpointNotFound", %{}, id: "not-found") %>
    """
  end

  defp render_action(:show, %{show_endpoint: %Query{}} = assigns) do
    ~H"""
    <%= live_react_component(
      "Interfaces.ShowEndpoint",
      %{
        baseUrl: @base_url,
        endpoint: @show_endpoint,
        declaredParams: @declared_params,
        queryResultRows: @query_result_rows
      },
      id: "show-endpoint"
    ) %>
    """
  end

  defp render_action(:edit, assigns) do
    ~H"""
    <%= live_react_component(
      "Interfaces.EndpointEditor",
      %{
        endpoint: @show_endpoint,
        queryResultRows: @query_result_rows,
        declaredParams: @declared_params,
        parseErrorMessage: @parse_error_message
      },
      id: "edit-endpoint"
    ) %>
    """
  end

  defp render_action(:new, assigns) do
    ~H"""
    <%= live_react_component(
      "Interfaces.EndpointEditor",
      %{
        queryResultRows: @query_result_rows,
        declaredParams: @declared_params,
        parseErrorMessage: @parse_error_message
      },
      id: "new-endpoint"
    ) %>
    """
  end

  defp render_action(_, assigns) do
    ~H""
  end

  def mount(%{}, %{"user_id" => user_id}, socket) do
    endpoints = Endpoints.list_endpoints_by(user_id: user_id)
    user = Users.get(user_id)

    allow_access =
      Enum.any?([
        Utils.flag("endpointsOpenBeta"),
        user.endpoints_beta
      ])

    {:ok,
     socket
     |> assign(:endpoints, endpoints)
     |> assign(:user_id, user_id)
     |> assign(:user, user)
     |> assign(:query_result_rows, nil)
     |> assign(:show_endpoint, nil)
     |> assign(:allow_access, allow_access)
     |> assign(:base_url, LogflareWeb.Endpoint.url())
     |> assign(:parse_error_message, nil)
     |> assign(:declared_params, [])}
  end

  def handle_params(params, _uri, socket) do
    endpoint_id = params["id"]

    endpoint =
      if endpoint_id do
        Endpoints.get_by(id: endpoint_id, user_id: socket.assigns.user_id)
      end

    socket =
      socket
      |> assign(:show_endpoint, endpoint)
      |> then(fn
        socket when endpoint != nil ->
          {:ok, %{parameters: parameters}} = Endpoints.parse_query_string(endpoint.query)

          assign(socket, :declared_params, parameters)

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
    {:noreply, push_patch(socket, to: Routes.endpoints_path(socket, :edit, id))}
  end

  def handle_event("list-endpoints", _params, socket) do
    {:noreply, push_patch(socket, to: Routes.endpoints_path(socket, :index))}
  end

  def handle_event(
        "delete-endpoint",
        %{"endpoint_id" => id},
        %{assigns: assigns} = socket
      ) do
    endpoint = Endpoints.get_endpoint_query(id)
    {:ok, _} = Endpoints.delete_query(endpoint)
    endpoints = Endpoints.list_endpoints_by(user_id: assigns.user_id)

    {:noreply,
     socket
     |> assign(:endpoints, endpoints)
     |> assign(:show_endpoint, nil)
     |> put_flash(
       :info,
       "#{endpoint.name} has been deleted"
     )
     |> push_patch(to: "/endpoints")}
  end

  def handle_event(
        "show-endpoint",
        %{"endpoint_id" => id},
        %{assigns: %{endpoints: endpoints}} = socket
      ) do
    endpoint = Enum.find(endpoints, fn e -> e.id == id end)

    {:noreply, push_patch(socket, to: "/endpoints/#{endpoint.id}")}
  end

  def handle_event(
        "run-query",
        %{"endpoint_id" => endpoint_id, "query_params" => query_params},
        socket
      ) do
    query = Endpoints.get_endpoint_query(endpoint_id)
    result = Endpoints.run_query(query, query_params)
    socket = handle_query_result(socket, result)

    {:noreply, socket}
  end

  def handle_event(
        "run-query",
        %{"query_params" => query_params},
        %{assigns: %{show_endpoint: %Query{} = query}} = socket
      ) do
    result = Endpoints.run_query(query, query_params)
    socket = handle_query_result(socket, result)
    {:noreply, socket}
  end

  def handle_event(
        "run-query",
        %{"query_params" => query_params, "query_string" => query_string},
        %{assigns: %{user: user}} = socket
      ) do
    result = Endpoints.run_query_string(user, {:bq_sql, query_string}, query_params)
    socket = handle_query_result(socket, result)
    {:noreply, socket}
  end

  def handle_event("parse-query", %{"query_string" => query_string}, socket) do
    socket =
      case Endpoints.parse_query_string(query_string) do
        {:ok, %{parameters: params_list}} ->
          socket
          |> assign(:declared_params, params_list)
          |> assign(:parse_error_message, nil)

        {:error, err} ->
          socket
          |> assign(:parse_error_message, if(is_binary(err), do: err, else: inspect(err)))
      end

    {:noreply, socket}
  end

  def handle_event("new-endpoint", _, socket) do
    {:noreply, socket |> assign(:show_endpoint, nil) |> push_patch(to: "/endpoints/new")}
  end

  def handle_event("apply-beta", _params, %{assigns: %{user: user}} = socket) do
    Logger.info("Endpoints application submitted.", %{user: %{id: user.id, email: user.email}})

    {:noreply,
     socket
     |> put_flash(:info, "Successfully applied for the Endpoints beta. We'll be in touch!")}
  end

  defp handle_query_result(socket, {:ok, %{rows: rows}}) do
    socket
    |> put_flash(:info, "Ran query successfully")
    |> assign(:query_result_rows, rows)
  end

  defp handle_query_result(socket, {:error, err}) do
    socket
    |> put_flash(:error, "Error occured when running query: #{inspect(err)}")
    |> assign(:query_result, nil)
  end
end
