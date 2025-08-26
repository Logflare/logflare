defmodule LogflareWeb.EndpointsLive do
  @moduledoc false
  use LogflareWeb, :live_view
  use Phoenix.Component

  require Logger

  alias Logflare.Endpoints
  alias Logflare.Users
  alias LogflareWeb.{QueryComponents, Utils}

  embed_templates("actions/*", suffix: "_action")
  embed_templates("components/*")

  def render(%{allow_access: false} = assigns), do: closed_beta_action(assigns)
  def render(%{live_action: :index} = assigns), do: index_action(assigns)
  def render(%{live_action: :show, show_endpoint: nil} = assigns), do: not_found_action(assigns)
  def render(%{live_action: :show} = assigns), do: show_action(assigns)
  def render(%{live_action: :new} = assigns), do: new_action(assigns)
  def render(%{live_action: :edit} = assigns), do: edit_action(assigns)

  defp render_docs_link(assigns) do
    ~H"""
    <.subheader_link to="https://docs.logflare.app/concepts/endpoints" external={true} text="docs" fa_icon="book" />
    """
  end

  defp render_access_tokens_link(assigns) do
    ~H"""
    <.subheader_link to={~p"/access-tokens"} text="access tokens" fa_icon="key" />
    """
  end

  def mount(%{}, %{"user_id" => user_id}, socket) do
    user = Users.get(user_id)

    allow_access = Enum.any?([Utils.flag("endpointsOpenBeta"), user.endpoints_beta])

    alerts = Endpoints.list_endpoints_by(user_id: user.id)

    socket =
      socket
      |> assign(:user_id, user_id)
      |> assign(:user, user)
      #  must be below user_id assign
      |> refresh_endpoints()
      |> assign(:query_result_rows, nil)
      |> assign(:total_bytes_processed, nil)
      |> assign(:show_endpoint, nil)
      |> assign(:endpoint_changeset, Endpoints.change_query(%Endpoints.Query{}))
      |> assign(:allow_access, allow_access)
      |> assign(:base_url, LogflareWeb.Endpoint.url())
      |> assign(:parse_error_message, nil)
      |> assign(:query_string, nil)
      |> assign(:prev_params, %{})
      |> assign(:params_form, to_form(%{"query" => "", "params" => %{}}, as: "run"))
      |> assign(:declared_params, %{})
      |> assign(:alerts, alerts)
      |> assign_sources()
      |> assign(:parsed_result, nil)

    {:ok, socket}
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
          {:ok, parsed_result} =
            Endpoints.parse_query_string(
              :bq_sql,
              endpoint.query,
              Enum.filter(socket.assigns.endpoints, &(&1.id != endpoint.id)),
              socket.assigns.alerts
            )

          socket
          |> assign_updated_params_form(parsed_result.parameters, parsed_result.expanded_query)
          # set changeset
          |> assign(:endpoint_changeset, Endpoints.change_query(endpoint, %{}))
          |> assign(:parsed_result, parsed_result)

        # index page
        %{assigns: %{live_action: :index}} = socket ->
          socket
          |> refresh_endpoints()
          |> assign(:endpoint_changeset, nil)
          |> assign(:query_result_rows, nil)

        other ->
          other
          # reset the changeset
          |> assign(
            :endpoint_changeset,
            Endpoints.change_query(%Endpoints.Query{query: placeholder_sql()})
          )
          # reset test results
          |> assign(:query_result_rows, nil)
      end)

    {:noreply, socket}
  end

  def handle_event(
        "save-endpoint",
        %{"endpoint" => params},
        %{assigns: %{user: user, show_endpoint: show_endpoint}} = socket
      ) do
    Logger.debug("Saving endpoint", params: params)

    case upsert_query(show_endpoint, user, params) do
      {:ok, endpoint} ->
        verb = if show_endpoint, do: "updated", else: "created"

        {:noreply,
         socket
         |> put_flash(:info, "Successfully #{verb} endpoint #{endpoint.name}")
         |> push_patch(to: ~p"/endpoints/#{endpoint.id}")
         |> assign(:show_endpoint, endpoint)}

      {:error, %Ecto.Changeset{} = changeset} ->
        verb = if(show_endpoint, do: "update", else: "create")
        message = "Could not #{verb} endpoint. Please fix the errors before trying again."

        socket =
          socket
          |> put_flash(:info, message)
          |> assign(:endpoint_changeset, changeset)

        {:noreply, socket}
    end
  end

  def handle_event(
        "delete-endpoint",
        %{"endpoint_id" => id},
        %{assigns: _assigns} = socket
      ) do
    endpoint = Endpoints.get_endpoint_query(id)
    {:ok, _} = Endpoints.delete_query(endpoint)

    {:noreply,
     socket
     |> refresh_endpoints()
     |> assign(:show_endpoint, nil)
     |> put_flash(:info, "#{endpoint.name} has been deleted")
     |> push_patch(to: "/endpoints")}
  end

  def handle_event(
        "run-query",
        %{"run" => payload},
        %{assigns: %{user: user}} = socket
      ) do
    query_string = Map.get(payload, "query")
    query_params = Map.get(payload, "params", %{})

    allowed_labels = Ecto.Changeset.get_field(socket.assigns.endpoint_changeset, :labels)

    parsed_labels =
      Endpoints.parse_labels(allowed_labels, "", query_params)
      |> Map.merge(%{
        "endpoint_id" => socket.assigns.endpoint_changeset.data.id
      })

    case Endpoints.run_query_string(user, {:bq_sql, query_string},
           params: query_params,
           parsed_labels: parsed_labels,
           use_query_cache: false
         ) do
      {:ok, %{rows: rows, total_bytes_processed: total_bytes_processed}} ->
        {:noreply,
         socket
         |> put_flash(:info, "Ran query successfully")
         |> assign(:prev_params, query_params)
         |> assign(:query_result_rows, rows)
         |> assign(:total_bytes_processed, total_bytes_processed)}

      {:error, err} ->
        {:noreply,
         socket
         |> put_flash(:error, "Error occured when running query: #{inspect(err)}")}
    end
  end

  def handle_event("apply-beta", _params, %{assigns: %{user: user}} = socket) do
    Logger.debug("Endpoints application submitted.", %{user: %{id: user.id, email: user.email}})

    message = "Successfully applied for the Endpoints beta. We'll be in touch!"
    {:noreply, put_flash(socket, :info, message)}
  end

  def handle_event("validate", %{"_target" => ["live_monaco_editor", _]}, socket) do
    # ignore change events from the editor field
    {:noreply, socket}
  end

  def handle_event("validate", _params, socket) do
    # noop
    {:noreply, socket}
  end

  def handle_info({:query_string_updated, query_string}, socket) do
    parsed_result =
      Endpoints.parse_query_string(
        :bq_sql,
        query_string,
        socket.assigns.endpoints,
        socket.assigns.alerts
      )

    socket =
      case parsed_result do
        {:ok, %{parameters: parameters, expanded_query: expanded_query}} ->
          socket
          |> assign_updated_params_form(parameters, expanded_query)

        _error ->
          socket
      end

    {:noreply, socket}
  end

  defp assign_updated_params_form(socket, parameters, query_string) do
    params = for(k <- parameters, do: {k, nil}, into: %{})
    form = to_form(%{"query" => query_string, "params" => params}, as: "run")

    socket
    |> assign(:query_string, query_string)
    |> assign(:declared_params, parameters)
    |> assign(:params_form, form)
  end

  defp refresh_endpoints(%{assigns: assigns} = socket) do
    endpoints =
      Endpoints.list_endpoints_by(user_id: assigns.user_id)
      |> Endpoints.calculate_endpoint_metrics()

    assign(socket, :endpoints, endpoints)
  end

  defp assign_sources(socket) do
    %{user_id: user_id} = socket.assigns

    sources = Logflare.Sources.list_sources_by_user(user_id)

    assign(socket, sources: sources)
  end

  defp upsert_query(show_endpoint, user, params) do
    case show_endpoint do
      nil -> Endpoints.create_query(user, params)
      %_{} -> Endpoints.update_query(show_endpoint, params)
    end
  end

  defp placeholder_sql,
    do: """
    select timestamp, event_message from YourApp.SourceName
    """
end
