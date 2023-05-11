defmodule LogflareWeb.EndpointsLive do
  @moduledoc false
  use LogflareWeb, :live_view
  require Logger
  alias Logflare.Endpoints
  alias Logflare.Endpoints.Query
  alias Logflare.Users
  alias LogflareWeb.Utils

  def render(%{allow_access: false} = assigns) do
    ~L"""
    <div class="container tw-mx-auto tw-mt-5">

    <div class="col-lg-6 tw-mb-4 tw-mx-auto">
      <h3 class="tw-text-white">Logflare Endpoints Beta</h3>
      <p>We're in the process of releasing a major feature called Logflare Endpoints.</p>
      <p>Endpoints lets you write ANSI SQL against your structured logs and create API endpoints from the results.</p>
      <p>With Endpoints you can operationalize structured timestamped events and integrate your data into an end-user facing
        application without any other complicated data pipelines or batch processing aggregations.</p>

      <h3 class="tw-text-white">Apply</h3>
      <p>If this sounds intereseting to you just click the button below and we'll get in touch.</p>
      <button class="btn btn-primary" phx-click="apply-beta">I'm interested!</button>
    </div>
    </div>
    """
  end

  def render(%{allow_access: true} = assigns) do
    ~L"""
    <%= live_react_component("Comp.SubHeader", %{
      paths: [%{to: "/endpoints", label: "endpoints"}],
      actions: [
        %{to: Routes.access_tokens_path(@socket, :index), html: ~L(<i class="fas fa-key"></i> Manage access tokens</span>) |> safe_to_string() }
      ]
      }, [id: "subheader"])
    %>
    <div class="tw-flex tw-flex-row tw-py-10 tw-px-4 h-full">
    <section>
      <%= live_react_component("Interfaces.EndpointsBrowserList", %{
          endpoints: @endpoints,
          selectedEndpoint: @show_endpoint
          }, [id: "endpoints-browser-list"])
      %>
    </section>

      <section class="tw-flex-grow">
          <%= render_action(assigns.live_action, assigns) %>
      </section>
    </div>
    """
  end

  defp render_action(:index, _assigns) do
    assigns = %{}
    ~L"""
    <%= live_react_component("Interfaces.EndpointsIntro", %{}, [id: "endpoints-intro"]) %>
    """
  end

  defp render_action(:show, %{show_endpoint: nil} = assigns) do
    assigns = %{}
    ~L"""
    <%= live_react_component("Interfaces.EndpointNotFound", %{}, [id: "not-found"]) %>
    """
  end

  defp render_action(:show, %{show_endpoint: %Query{}} = assigns) do
      assigns = %{}
      ~L"""
    <%= live_react_component("Interfaces.ShowEndpoint", %{baseUrl: @base_url, endpoint: @show_endpoint, declaredParams: @declared_params, queryResultRows: @query_result_rows}, [id: "show-endpoint"]) %>
    """
  end

  defp render_action(:edit, assigns) do
    assigns = %{}
    ~L"""
    <%= live_react_component("Interfaces.EndpointEditor", %{endpoint: @show_endpoint, queryResultRows: @query_result_rows, declaredParams: @declared_params, parseErrorMessage: @parse_error_message}, [id: "edit-endpoint"]) %>
    """
  end

  defp render_action(:new, assigns) do
    assigns = %{}
    ~L"""
    <%= live_react_component("Interfaces.EndpointEditor", %{queryResultRows: @query_result_rows, declaredParams: @declared_params, parseErrorMessage: @parse_error_message}, [id: "new-endpoint"]) %>
    """
  end

  defp render_action(_, assigns) do
    assigns = %{}
    ~L""
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
      else
        nil
      end

    socket =
      socket
      |> assign(:show_endpoint, endpoint)
      |> then(fn
        socket when endpoint != nil ->
          {:ok, %{parameters: parameters}} = Endpoints.parse_query_string(endpoint.query)

          socket |> assign(:declared_params, parameters)

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

  def handle_event("list-endpoints", _params, socket) do
    {:noreply, socket |> push_patch(to: Routes.endpoints_path(socket, :index))}
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

    {:noreply,
     socket
     |> push_patch(to: "/endpoints/#{endpoint.id}")}
  end

  def handle_event(
        "run-query",
        %{"query_string" => query_string, "query_params" => query_params},
        %{assigns: %{user: user}} = socket
      ) do
    case Endpoints.run_query_string(user, query_string, params: query_params) do
      {:ok, %{rows: rows}} ->
        {:noreply,
         socket
         |> put_flash(:info, "Ran query successfully")
         |> assign(:query_result_rows, rows)}

      {:error, err} ->
        {:noreply,
         socket
         |> put_flash(:error, "Error occured when running query: #{inspect(err)}")
         |> assign(:query_result, nil)}
    end
  end

  def handle_event(
        "run-query",
        %{"query_params" => _} = params,
        %{assigns: %{show_endpoint: %Query{} = endpoint}} = socket
      ) do
    params = Map.put(params, "query_string", endpoint.query)
    handle_event("run-query", params, socket)
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

  def handle_event(
        "new-endpoint",
        _,
        socket
      ) do
    {:noreply, socket |> assign(:show_endpoint, nil) |> push_patch(to: "/endpoints/new")}
  end

  def handle_event("apply-beta", _params, %{assigns: %{user: user}} = socket) do
    Logger.info("Endpoints application submitted.", %{user: %{id: user.id, email: user.email}})

    {:noreply,
     socket
     |> put_flash(:info, "Successfully applied for the Endpoints beta. We'll be in touch!")}
  end
end
