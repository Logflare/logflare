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
  alias LogflareWeb.Endpoints.RunQuery
  alias LogflareWeb.QueryErrorHelpers
  alias Logflare.Utils

  @test_form_defaults %{
    "query" => "",
    "params" => %{},
    "reservation" => nil,
    "query_mode" => "sql",
    "sandbox_query" => "",
    "show_transformed" => false
  }

  embed_templates("actions/*", suffix: "_action")

  def render(%{allow_access: false} = assigns), do: closed_beta_action(assigns)
  def render(%{live_action: :index} = assigns), do: index_action(assigns)

  def render(%{live_action: action, show_endpoint: nil} = assigns) when action in [:show, :edit],
    do: not_found_action(assigns)

  def render(%{live_action: :show} = assigns), do: show_action(assigns)
  def render(%{live_action: :new} = assigns), do: new_action(assigns)
  def render(%{live_action: :edit} = assigns), do: edit_action(assigns)

  def mount(%{}, _session, socket) do
    %{assigns: %{user: user}} = socket

    allow_access = Enum.any?([Utils.flag("endpointsOpenBeta"), user.endpoints_beta])

    alerts = Endpoints.list_endpoints_by(user_id: user.id)

    socket =
      socket
      |> assign(:user_id, user.id)
      #  must be below user_id assign
      |> refresh_endpoints()
      |> assign(:allow_access, allow_access)
      |> assign(:alerts, alerts)
      |> assign_sources()
      |> assign_backends()

    {:ok, socket}
  end

  def handle_params(params, _uri, socket) do
    socket =
      socket
      |> assign(
        show_endpoint: nil,
        endpoint_changeset: nil,
        parsed_result: nil,
        params_form: test_form(),
        declared_params: [],
        test_result: nil
      )
      |> apply_action(socket.assigns.live_action, params)

    {:noreply, socket}
  end

  defp apply_action(socket, :index, _params), do: refresh_endpoints(socket)

  defp apply_action(socket, :new, params) do
    params =
      Map.replace_lazy(params, "query", fn sql ->
        {:ok, formatted} = Sql.format(sql)
        formatted
      end)

    assign(socket, :endpoint_changeset, Endpoints.change_query(%EndpointQuery{}, params))
  end

  defp apply_action(socket, action, %{"id" => id}) when action in [:show, :edit] do
    user = socket.assigns.team_user || socket.assigns.user

    case Endpoints.get_endpoint_query_by_user_access(user, id) do
      nil ->
        socket

      endpoint ->
        {:ok, parsed_result} =
          Endpoints.parse_query_string(
            endpoint.language,
            endpoint.query,
            Enum.filter(socket.assigns.endpoints, &(&1.id != endpoint.id)),
            socket.assigns.alerts
          )

        socket
        |> assign(:show_endpoint, endpoint)
        |> assign_updated_params_form(parsed_result.parameters, parsed_result.expanded_query)
        |> assign(:endpoint_changeset, Endpoints.change_query(endpoint, %{}))
        |> assign(:parsed_result, parsed_result)
    end
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
    payload = Map.put_new(payload, "query", socket.assigns.params_form[:query].value)
    socket = assign(socket, :params_form, test_form(payload))

    socket =
      if sandbox_query?(socket, payload) do
        run_sandbox_query(socket, payload)
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
      changeset =
        socket.assigns.endpoint_changeset.data
        |> Endpoints.change_query(endpoint_params)
        |> Map.put(:action, :validate)

      {:noreply, assign(socket, :endpoint_changeset, changeset)}
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
    changeset = socket.assigns.endpoint_changeset
    params = Map.put(changeset.params || %{}, "query", query_string)

    changeset =
      changeset.data
      |> Endpoints.change_query(params)
      |> Map.put(:action, :validate)

    socket = assign(socket, :endpoint_changeset, changeset)

    parsed_result =
      if query_string != "" do
        socket
        |> get_current_endpoint_language()
        |> Endpoints.parse_query_string(
          query_string,
          socket.assigns.endpoints,
          socket.assigns.alerts
        )
      end

    socket =
      case parsed_result do
        {:ok, %{parameters: parameters, expanded_query: expanded_query}} ->
          socket
          |> assign_updated_params_form(
            parameters,
            expanded_query,
            socket.assigns.params_form.params
          )

        _error ->
          socket
      end

    {:noreply, socket}
  end

  defp assign_updated_params_form(socket, parameters, query_string, values \\ %{}) do
    previous_params = Map.get(values, "params", %{})
    params = Map.new(parameters, fn key -> {key, Map.get(previous_params, key)} end)
    values = Map.merge(values, %{"query" => query_string, "params" => params})

    socket
    |> assign(:declared_params, parameters)
    |> assign(:params_form, test_form(values))
  end

  defp run_endpoint_query(
         %{assigns: %{user: %Logflare.User{} = user}} = socket,
         %{"query" => query_string} = payload
       )
       when is_binary(query_string) do
    query_params = Map.get(payload, "params", %{})
    reservation = Map.get(payload, "reservation")

    allowed_labels = Ecto.Changeset.get_field(socket.assigns.endpoint_changeset, :labels)

    parsed_labels =
      Endpoints.parse_labels(allowed_labels, "", query_params)
      |> Map.merge(%{
        "endpoint_id" => socket.assigns.endpoint_changeset.data.id
      })

    redact_pii = Ecto.Changeset.get_field(socket.assigns.endpoint_changeset, :redact_pii)
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
        |> assign(:test_result, successful_test_result(:endpoint, result, nil))

      {:error, err} ->
        message = if is_binary(err), do: err, else: QueryErrorHelpers.query_error_message(err)
        assign(socket, :test_result, %{kind: :endpoint, status: :error, error: message})
    end
  end

  defp run_sandbox_query(%{assigns: %{show_endpoint: endpoint}} = socket, payload) do
    show_transformed? = Map.get(payload, "show_transformed") == "true"
    sandbox_query = Map.get(payload, "sandbox_query")
    query_params = Map.get(payload, "params", %{})
    query_mode = Map.get(payload, "query_mode", "sql")
    reservation = Map.get(payload, "reservation")

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

    case Endpoints.run_query(endpoint, sandbox_params, reservation: reservation) do
      {:ok, result} ->
        transformed_query = maybe_transformed_query(show_transformed?, endpoint, sandbox_params)

        socket
        |> put_flash(:info, "Ran sandbox query successfully")
        |> assign(:test_result, successful_test_result(:sandbox, result, transformed_query))

      {:error, error} ->
        Logger.error(
          "Sandbox query failed: '#{inspect(error)}', endpoint_id: #{endpoint.id}, backend_id: #{endpoint.backend_id}, sandbox_params: '#{inspect(sandbox_params)}'"
        )

        socket
        |> put_flash(:error, "Error occurred when running sandbox query")
        |> assign(:test_result, %{
          kind: :sandbox,
          status: :error,
          error: "Please verify your query syntax."
        })
    end
  end

  defp sandbox_query?(
         %{assigns: %{show_endpoint: %{sandboxable: true}}},
         %{"sandbox_query" => sandbox_query}
       )
       when is_binary(sandbox_query),
       do: String.trim(sandbox_query) != ""

  defp sandbox_query?(_socket, _payload), do: false

  defp test_form(overrides \\ %{}) do
    @test_form_defaults
    |> Map.merge(overrides)
    |> to_form(as: "run")
  end

  @spec successful_test_result(
          :endpoint | :sandbox,
          %{required(:rows) => [term()], optional(atom()) => term()},
          String.t() | nil
        ) :: map()
  defp successful_test_result(kind, %{rows: rows} = result, transformed_query) do
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
  end

  defp get_current_endpoint_language(%{assigns: assigns}) do
    case Ecto.Changeset.get_field(assigns.endpoint_changeset, :backend_id) do
      nil ->
        EndpointQuery.map_backend_to_language(
          assigns.default_backend,
          SingleTenant.supabase_mode?()
        )

      backend_id ->
        Endpoints.derive_language_from_backend_id(backend_id)
    end
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

  defp format_query_language(:bq_sql), do: "BigQuery SQL"
  defp format_query_language(:ch_sql), do: "ClickHouse SQL"
  defp format_query_language(:pg_sql), do: "Postgres SQL"
  defp format_query_language(language), do: language |> to_string() |> String.upcase()
end
