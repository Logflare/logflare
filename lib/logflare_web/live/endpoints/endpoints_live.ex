defmodule LogflareWeb.EndpointsLive do
  @moduledoc false

  use LogflareWeb, :live_view
  use Phoenix.Component

  import Ecto.Query, only: [from: 2]

  require Logger

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Backend
  alias Logflare.Endpoints
  alias Logflare.Endpoints.EndpointQuery
  alias Logflare.SingleTenant
  alias Logflare.Sql
  alias LogflareWeb.Endpoints.Components
  alias LogflareWeb.QueryComponents
  alias LogflareWeb.QueryErrorHelpers
  alias Logflare.Utils

  @test_form_defaults %{
    "query" => "",
    "params" => %{},
    "reservation" => nil,
    "query_mode" => "sql",
    "consumer_query" => "",
    "show_transformed" => false
  }

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
      |> assign(:test_result, nil)
      |> assign(:show_endpoint, nil)
      |> assign(:endpoint_changeset, Endpoints.change_query(%Endpoints.EndpointQuery{}))
      |> assign(:selected_backend_id, nil)
      |> assign(:allow_access, allow_access)
      |> assign(:parse_error_message, nil)
      |> assign(:query_string, nil)
      |> assign(:params_form, test_form())
      |> assign(:declared_params, [])
      |> assign(:alerts, alerts)
      |> assign_sources()
      |> assign_backends()
      |> assign(:parsed_result, nil)
      |> assign(:redact_pii, false)

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
      |> then(fn
        socket when endpoint != nil ->
          {:ok, parsed_result} =
            Endpoints.parse_query_string(
              endpoint.language,
              endpoint.query,
              Enum.filter(socket.assigns.endpoints, &(&1.id != endpoint.id)),
              socket.assigns.alerts
            )

          socket
          |> assign_updated_params_form(parsed_result.parameters, parsed_result.expanded_query)
          |> assign(:endpoint_changeset, Endpoints.change_query(endpoint, %{}))
          |> assign(:selected_backend_id, endpoint.backend_id)
          |> assign(:parsed_result, parsed_result)
          |> assign(:redact_pii, endpoint.redact_pii || false)
          |> assign(:test_result, nil)

        # index page
        %{assigns: %{live_action: :index}} = socket ->
          socket
          |> refresh_endpoints()
          |> assign(:endpoint_changeset, nil)
          |> assign(:test_result, nil)

        %{assigns: %{live_action: :new}} = socket ->
          params =
            Map.replace_lazy(params, "query", fn sql ->
              {:ok, formatted} = Sql.format(sql)
              formatted
            end)

          changeset =
            %Endpoints.EndpointQuery{}
            |> Endpoints.change_query(params)

          socket
          |> assign(:endpoint_changeset, changeset)
          |> assign(:params_form, test_form())
          |> assign(:declared_params, [])
          |> assign(:test_result, nil)

        other ->
          other
          # reset the changeset
          |> assign(
            :endpoint_changeset,
            Endpoints.change_query(%Endpoints.EndpointQuery{query: placeholder_sql()})
          )
          |> assign(:selected_backend_id, nil)
          |> assign(:redact_pii, false)
          |> assign(:test_result, nil)
      end)

    {:noreply, socket}
  end

  def handle_event(
        "save-endpoint",
        %{"endpoint" => params},
        %{assigns: %{user: user, show_endpoint: show_endpoint, team: team}} = socket
      ) do
    Logger.debug("Saving endpoint", params: params)
    origin = socket.assigns.team_user || user

    with :ok <- authorize_backend_id(origin, params),
         {:ok, endpoint} <- upsert_query(show_endpoint, user, origin, params) do
      verb = if show_endpoint, do: "updated", else: "created"

      {:noreply,
       socket
       |> put_flash(:info, "Successfully #{verb} endpoint #{endpoint.name}")
       |> push_patch(to: LogflareWeb.Utils.with_team_param(~p"/endpoints/#{endpoint.id}", team))
       |> assign(:show_endpoint, endpoint)
       |> assign(:test_result, nil)}
    else
      {:error, %Ecto.Changeset{} = changeset} ->
        verb = if(show_endpoint, do: "update", else: "create")
        message = "Could not #{verb} endpoint. Please fix the errors before trying again."

        socket =
          socket
          |> put_flash(:info, message)
          |> assign(:endpoint_changeset, changeset)
          |> assign(:selected_backend_id, changeset.data.backend_id)

        {:noreply, socket}

      {:error, :backend_not_found} ->
        {:noreply, put_flash(socket, :error, "Backend not found")}
    end
  end

  def handle_event(
        "delete-endpoint",
        %{"endpoint_id" => id},
        %{assigns: assigns} = socket
      ) do
    user = assigns[:team_user] || assigns[:user]

    case Endpoints.get_endpoint_query_by_user_access(user, id) do
      nil ->
        {:noreply, put_flash(socket, :error, "You do not have access to that endpoint.")}

      endpoint ->
        {:ok, _} = Endpoints.delete_query(endpoint, user)

        {:noreply,
         socket
         |> refresh_endpoints()
         |> assign(:show_endpoint, nil)
         |> put_flash(:info, "#{endpoint.name} has been deleted")
         |> push_patch(to: "/endpoints")}
    end
  end

  def handle_event(
        "run-query",
        %{"run" => payload},
        socket
      ) do
    payload = Map.put_new(payload, "query", socket.assigns.query_string)
    socket = assign(socket, :params_form, test_form(payload))

    socket =
      if consumer_query?(socket, payload) do
        run_consumer_query(socket, payload)
      else
        run_endpoint_query(socket, payload)
      end

    {:noreply, socket}
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
    origin = socket.assigns[:team_user] || socket.assigns.user

    with :ok <- authorize_backend_id(origin, endpoint_params) do
      selected_backend_id = Map.get(endpoint_params, "backend_id")

      changeset =
        socket.assigns.endpoint_changeset.data
        |> Endpoints.change_query(endpoint_params)
        |> Map.put(:action, :validate)

      redact_pii = Map.get(endpoint_params, "redact_pii") == "true"

      {:noreply,
       socket
       |> assign(:endpoint_changeset, changeset)
       |> assign(:selected_backend_id, selected_backend_id)
       |> assign(:redact_pii, redact_pii)
       |> assign_determined_language()}
    else
      {:error, :backend_not_found} ->
        {:noreply, put_flash(socket, :error, "Backend not found")}
    end
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

    socket
    |> assign(:query_string, query_string)
    |> assign(:declared_params, parameters)
    |> assign(:params_form, test_form(%{"query" => query_string, "params" => params}))
  end

  defp run_endpoint_query(%{assigns: %{user: user}} = socket, payload) do
    query_string = Map.get(payload, "query")
    query_params = Map.get(payload, "params", %{})
    reservation = Map.get(payload, "reservation")

    allowed_labels = Ecto.Changeset.get_field(socket.assigns.endpoint_changeset, :labels)

    parsed_labels =
      Endpoints.parse_labels(allowed_labels, "", query_params)
      |> Map.merge(%{
        "endpoint_id" => socket.assigns.endpoint_changeset.data.id
      })

    redact_pii = socket.assigns.redact_pii
    backend_id = Ecto.Changeset.get_field(socket.assigns.endpoint_changeset, :backend_id)

    endpoint_language = get_current_endpoint_language(socket)

    case Endpoints.run_query_string(user, {endpoint_language, query_string},
           params: query_params,
           parsed_labels: parsed_labels,
           use_query_cache: false,
           redact_pii: redact_pii,
           backend_id: backend_id,
           reservation: reservation
         ) do
      {:ok, result} ->
        socket
        |> put_flash(:info, "Ran query successfully")
        |> assign(:test_result, successful_test_result(:endpoint, result))

      {:error, err} ->
        message = if is_binary(err), do: err, else: QueryErrorHelpers.query_error_message(err)
        assign(socket, :test_result, %{kind: :endpoint, status: :error, error: message})
    end
  end

  defp run_consumer_query(%{assigns: %{show_endpoint: endpoint}} = socket, payload) do
    show_transformed? = Map.get(payload, "show_transformed") == "true"
    consumer_query = Map.get(payload, "consumer_query")
    query_params = Map.get(payload, "params", %{})
    query_mode = Map.get(payload, "query_mode", "sql")
    reservation = Map.get(payload, "reservation")

    sandbox_params =
      case query_mode do
        "sql" -> Map.put(query_params, "sql", consumer_query)
        "lql" -> Map.put(query_params, "lql", consumer_query)
        _ -> query_params
      end

    Logger.metadata(
      endpoint_id: endpoint.id,
      backend_id: endpoint.backend_id,
      sandbox_params: sandbox_params,
      user_id: endpoint.user_id
    )

    case Endpoints.run_query(endpoint, sandbox_params, reservation: reservation) do
      {:ok, result} ->
        transformed_query = maybe_transformed_query(show_transformed?, endpoint, sandbox_params)

        socket
        |> put_flash(:info, "Ran consumer query successfully")
        |> assign(:test_result, successful_test_result(:consumer, result, transformed_query))

      {:error, error} ->
        Logger.error(
          "Sandbox query failed: '#{inspect(error)}', endpoint_id: #{endpoint.id}, backend_id: #{endpoint.backend_id}, sandbox_params: '#{inspect(sandbox_params)}'"
        )

        socket
        |> put_flash(:error, "Error occurred when running consumer query")
        |> assign(:test_result, %{
          kind: :consumer,
          status: :error,
          error: "Please verify your query syntax."
        })
    end
  end

  defp consumer_query?(
         %{assigns: %{show_endpoint: %{sandboxable: true}}},
         %{"consumer_query" => consumer_query}
       )
       when is_binary(consumer_query),
       do: String.trim(consumer_query) != ""

  defp consumer_query?(_socket, _payload), do: false

  defp test_form(overrides \\ %{}) do
    @test_form_defaults
    |> Map.merge(overrides)
    |> to_form(as: "run")
  end

  defp successful_test_result(kind, %{rows: rows} = result, transformed_query \\ nil) do
    %{
      kind: kind,
      status: :ok,
      rows: rows,
      total_bytes_processed: Map.get(result, :total_bytes_processed),
      transformed_query: transformed_query
    }
  end

  defp refresh_endpoints(%{assigns: assigns} = socket) do
    endpoints =
      Endpoints.list_endpoints_by(user_id: assigns.user_id)
      |> Logflare.Repo.preload(backend: from(b in Backend, select: struct(b, [:id, :name])))
      |> Endpoints.calculate_endpoint_metrics()

    assign(socket, :endpoints, endpoints)
  end

  defp authorize_backend_id(user, params) do
    case params["backend_id"] do
      v when v in [nil, ""] ->
        :ok

      backend_id ->
        if Backends.get_backend_by_user_access(user, backend_id),
          do: :ok,
          else: {:error, :backend_not_found}
    end
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
    default_backend = Backends.get_default_backend(user)

    socket
    |> assign(:backends, backends)
    |> assign(:default_backend, default_backend)
    |> assign(:show_backend_selection, show_backend_selection?)
    |> assign_determined_language()
  end

  defp get_current_endpoint_language(%{assigns: %{selected_backend_id: nil} = assigns}) do
    EndpointQuery.map_backend_to_language(assigns.default_backend, SingleTenant.supabase_mode?())
  end

  defp get_current_endpoint_language(%{assigns: %{selected_backend_id: selected_backend_id}}) do
    Endpoints.derive_language_from_backend_id(selected_backend_id)
  end

  defp assign_determined_language(socket) do
    socket
    |> assign(:determined_language, get_current_endpoint_language(socket))
  end

  defp maybe_transformed_query(false, _endpoint, _params), do: nil

  defp maybe_transformed_query(true, endpoint, params) do
    case Endpoints.get_transformed_query(endpoint, params) do
      {:ok, transformed} -> transformed
      _ -> nil
    end
  end

  defp upsert_query(show_endpoint, user, origin, params) do
    case show_endpoint do
      nil -> Endpoints.create_query(user, params, origin)
      %_{} -> Endpoints.update_query(origin, show_endpoint, params, origin)
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
end
