defmodule LogflareWeb.EndpointsLive do
  @moduledoc false
  use LogflareWeb, :live_view
  use Phoenix.Component

  require Logger

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Endpoints
  alias Logflare.Endpoints.PiiRedactor
  alias LogflareWeb.QueryComponents
  alias Logflare.Utils

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
    <.subheader_link team={@team} to={~p"/access-tokens"} text="access tokens" fa_icon="key" />
    """
  end

  def mount(%{}, _session, socket) do
    %{assigns: %{user: user}} = socket

    allow_access = Enum.any?([Utils.flag("endpointsOpenBeta"), user.endpoints_beta])

    alerts = Endpoints.list_endpoints_by(user_id: user.id)

    socket =
      socket
      |> assign(:user_id, user.id)
      #  must be below user_id assign
      |> refresh_endpoints()
      |> assign(:query_result_rows, nil)
      |> assign(:total_bytes_processed, nil)
      |> assign(:show_endpoint, nil)
      |> assign(:endpoint_changeset, Endpoints.change_query(%Endpoints.Query{}))
      |> assign(:selected_backend_id, nil)
      |> assign(:allow_access, allow_access)
      |> assign(:base_url, LogflareWeb.Endpoint.url())
      |> assign(:parse_error_message, nil)
      |> assign(:query_string, nil)
      |> assign(:prev_params, %{})
      |> assign(:params_form, to_form(%{"query" => "", "params" => %{}}, as: "run"))
      |> assign(:declared_params, %{})
      |> assign(:alerts, alerts)
      |> assign_sources()
      |> assign_backends()
      |> assign(:parsed_result, nil)
      |> assign(:redact_pii, false)
      |> assign(:sandbox_query, nil)
      |> assign(:sandbox_query_result_rows, nil)
      |> assign(:sandbox_total_bytes_processed, nil)
      |> assign(:sandbox_error, nil)
      |> assign(:show_transformed_query, false)
      |> assign(:transformed_sandbox_query, nil)
      |> assign(
        :sandbox_form,
        to_form(
          %{
            "query_mode" => "sql",
            "sandbox_query" => "",
            "params" => %{},
            "show_transformed" => false
          },
          as: "sandbox_form"
        )
      )

    {:ok, socket}
  end

  def handle_params(params, _uri, socket) do
    endpoint_id = params["id"]
    user = socket.assigns.team_user || socket.assigns.user

    endpoint =
      if endpoint_id do
        Endpoints.get_endpoint_query_by_user_access(user, endpoint_id)
      end

    socket =
      socket
      |> assign(:show_endpoint, endpoint)
      |> maybe_assign_team_context(params, endpoint)
      |> then(fn
        socket when endpoint != nil ->
          {:ok, parsed_result} =
            Endpoints.parse_query_string(
              endpoint.language,
              endpoint.query,
              Enum.filter(socket.assigns.endpoints, &(&1.id != endpoint.id)),
              socket.assigns.alerts
            )

          socket =
            socket
            |> assign_updated_params_form(parsed_result.parameters, parsed_result.expanded_query)
            # set changeset
            |> assign(:endpoint_changeset, Endpoints.change_query(endpoint, %{}))
            |> assign(:selected_backend_id, endpoint.backend_id)
            |> assign(:parsed_result, parsed_result)
            |> assign(:redact_pii, endpoint.redact_pii || false)

          # Clear test results when navigating to edit page
          if socket.assigns.live_action == :edit do
            socket
            |> assign(:query_result_rows, nil)
            |> assign(:total_bytes_processed, nil)
          else
            socket
          end

        # index page
        %{assigns: %{live_action: :index}} = socket ->
          socket
          |> refresh_endpoints()
          |> assign(:endpoint_changeset, nil)
          |> assign(:query_result_rows, nil)

        %{assigns: %{live_action: :new}} = socket ->
          params =
            Map.replace_lazy(params, "query", fn sql ->
              {:ok, formatted} = SqlFmt.format_query(sql)
              formatted
            end)

          changeset =
            %Endpoints.Query{}
            |> Endpoints.change_query(params)

          socket
          |> assign(:endpoint_changeset, changeset)

        other ->
          other
          # reset the changeset
          |> assign(
            :endpoint_changeset,
            Endpoints.change_query(%Endpoints.Query{query: placeholder_sql()})
          )
          |> assign(:selected_backend_id, nil)
          # reset test results
          |> assign(:query_result_rows, nil)
          |> assign(:redact_pii, false)
      end)

    {:noreply, socket}
  end

  def handle_event(
        "save-endpoint",
        %{"endpoint" => params},
        %{assigns: %{user: user, show_endpoint: show_endpoint, team: team}} = socket
      ) do
    Logger.debug("Saving endpoint", params: params)

    case upsert_query(show_endpoint, user, params) do
      {:ok, endpoint} ->
        verb = if show_endpoint, do: "updated", else: "created"

        {:noreply,
         socket
         |> put_flash(:info, "Successfully #{verb} endpoint #{endpoint.name}")
         |> push_patch(to: LogflareWeb.Utils.with_team_param(~p"/endpoints/#{endpoint.id}", team))
         |> assign(:show_endpoint, endpoint)
         |> assign(:query_result_rows, nil)
         |> assign(:total_bytes_processed, nil)}

      {:error, %Ecto.Changeset{} = changeset} ->
        verb = if(show_endpoint, do: "update", else: "create")
        message = "Could not #{verb} endpoint. Please fix the errors before trying again."

        socket =
          socket
          |> put_flash(:info, message)
          |> assign(:endpoint_changeset, changeset)
          |> assign(:selected_backend_id, changeset.data.backend_id)

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

    endpoint_language = get_current_endpoint_language(socket)
    redact_pii = socket.assigns.redact_pii
    backend_id = Ecto.Changeset.get_field(socket.assigns.endpoint_changeset, :backend_id)

    case Endpoints.run_query_string(user, {endpoint_language, query_string},
           params: query_params,
           parsed_labels: parsed_labels,
           use_query_cache: false,
           redact_pii: redact_pii,
           backend_id: backend_id
         ) do
      {:ok, %{rows: rows, total_bytes_processed: total_bytes_processed}} ->
        {:noreply,
         socket
         |> put_flash(:info, "Ran query successfully")
         |> assign(:prev_params, query_params)
         |> assign(:query_result_rows, rows)
         |> assign(:total_bytes_processed, total_bytes_processed)}

      {:ok, %{rows: rows}} ->
        # non-BQ results
        {:noreply,
         socket
         |> put_flash(:info, "Ran query successfully")
         |> assign(:prev_params, query_params)
         |> assign(:query_result_rows, rows)
         |> assign(:total_bytes_processed, nil)}

      {:error, err} ->
        {:noreply,
         socket
         |> put_flash(:error, "Error occured when running query: #{inspect(err)}")}
    end
  end

  def handle_event(
        "run-sandbox-query",
        %{"sandbox_form" => payload},
        %{assigns: %{show_endpoint: endpoint}} = socket
      ) do
    show_transformed? = Map.get(payload, "show_transformed") == "true"
    sandbox_query = Map.get(payload, "sandbox_query")
    query_params = Map.get(payload, "params", %{})
    query_mode = Map.get(payload, "query_mode", "sql")

    sandbox_params =
      case query_mode do
        "sql" -> Map.put(query_params, "sql", sandbox_query)
        "lql" -> Map.put(query_params, "lql", sandbox_query)
        _ -> query_params
      end

    Logger.metadata(
      endpoint_id: endpoint.id,
      backend_id: endpoint.backend_id,
      sandbox_params: sandbox_params,
      user_id: endpoint.user_id
    )

    # Update the form to preserve query mode and other inputs
    updated_form =
      to_form(
        %{
          "query_mode" => query_mode,
          "sandbox_query" => sandbox_query,
          "params" => query_params,
          "show_transformed" => show_transformed?
        },
        as: "sandbox_form"
      )

    case Endpoints.run_query(endpoint, sandbox_params) do
      {:ok, %{rows: rows} = result} ->
        total_bytes_processed = Map.get(result, :total_bytes_processed)

        socket =
          socket
          |> put_flash(:info, "Ran sandbox query successfully")
          |> assign(:sandbox_form, updated_form)
          |> assign(:sandbox_query_result_rows, rows)
          |> assign(:sandbox_total_bytes_processed, total_bytes_processed)
          |> assign(:sandbox_error, nil)
          |> assign(:show_transformed_query, show_transformed?)
          |> assign(:sandbox_query, sandbox_query)
          |> maybe_assign_transformed_query(show_transformed?, endpoint, sandbox_params)

        {:noreply, socket}

      {:error, error} ->
        Logger.error(
          "Sandbox query failed: '#{inspect(error)}', endpoint_id: #{endpoint.id}, backend_id: #{endpoint.backend_id}, sandbox_params: '#{inspect(sandbox_params)}'"
        )

        {:noreply,
         socket
         |> put_flash(:error, "Error occurred when running sandbox query")
         |> assign(:sandbox_form, updated_form)
         |> assign(:sandbox_error, "Please verify your query syntax.")
         |> assign(:sandbox_query_result_rows, nil)
         |> assign(:sandbox_total_bytes_processed, nil)
         |> assign(:sandbox_query, sandbox_query)}
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

  def handle_event("validate", %{"endpoint" => endpoint_params}, socket) do
    selected_backend_id = Map.get(endpoint_params, "backend_id")
    redact_pii = Map.get(endpoint_params, "redact_pii") == "true"

    changeset =
      socket.assigns.endpoint_changeset.data
      |> Endpoints.change_query(endpoint_params)
      |> Map.put(:action, :validate)

    socket =
      socket
      |> assign(:endpoint_changeset, changeset)
      |> assign(:selected_backend_id, selected_backend_id)
      |> assign(:redact_pii, redact_pii)
      |> assign_determined_language()

    {:noreply, socket}
  end

  def handle_event("validate", _params, socket) do
    # noop for other validation events
    {:noreply, socket}
  end

  def handle_info({:query_string_updated, query_string}, socket) do
    endpoint_language = get_current_endpoint_language(socket)

    parsed_result =
      Endpoints.parse_query_string(
        endpoint_language,
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

  defp assign_backends(socket) do
    %{user_id: user_id, user: user} = socket.assigns
    flag_enabled? = Utils.flag("endpointBackendSelection", user)

    backends =
      if flag_enabled? do
        Backends.list_backends_by_user_id(user_id)
        |> Enum.filter(&Adaptor.can_query?/1)
      else
        []
      end

    show_backend_selection? = flag_enabled? and backends != []
    determined_language = get_current_endpoint_language(socket)

    socket
    |> assign(:backends, backends)
    |> assign(:show_backend_selection, show_backend_selection?)
    |> assign(:determined_language, determined_language)
  end

  defp get_current_endpoint_language(%{assigns: assigns}) do
    case Map.get(assigns, :selected_backend_id) do
      nil -> :bq_sql
      backend_id -> Endpoints.derive_language_from_backend_id(backend_id)
    end
  end

  defp assign_determined_language(socket) do
    determined_language = get_current_endpoint_language(socket)
    assign(socket, :determined_language, determined_language)
  end

  defp maybe_assign_transformed_query(socket, false, _endpoint, _params), do: socket

  defp maybe_assign_transformed_query(socket, true, endpoint, params) do
    case Endpoints.get_transformed_query(endpoint, params) do
      {:ok, transformed} -> assign(socket, :transformed_sandbox_query, transformed)
      _ -> assign(socket, :transformed_sandbox_query, nil)
    end
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

  defp format_query_language(:bq_sql), do: "BigQuery SQL"
  defp format_query_language(:ch_sql), do: "ClickHouse SQL"
  defp format_query_language(:pg_sql), do: "Postgres SQL"
  defp format_query_language(language), do: language |> to_string() |> String.upcase()

  defp maybe_redact_query(query, redact_pii) when is_binary(query) do
    if redact_pii do
      PiiRedactor.redact_pii_from_value(query)
    else
      query
    end
  end

  defp maybe_redact_query(query, _redact_pii), do: query

  defp maybe_assign_team_context(socket, %{"t" => _team_id}, _endpoint), do: socket

  defp maybe_assign_team_context(socket, _params, endpoint) do
    LogflareWeb.AuthLive.assign_context_by_resource(socket, endpoint, socket.assigns.user.email)
  end
end
