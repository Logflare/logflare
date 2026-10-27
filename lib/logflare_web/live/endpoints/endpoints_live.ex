defmodule LogflareWeb.EndpointsLive do
  @moduledoc false

  use LogflareWeb, :live_view
  use Phoenix.Component

  import Ecto.Query, only: [from: 2]

  require Logger

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Backend
  alias Logflare.Backends.QueryError
  alias Logflare.Endpoints
  alias Logflare.Endpoints.EndpointQuery
  alias Logflare.SingleTenant
  alias Logflare.Sql
  alias Logflare.User
  alias LogflareWeb.Endpoints.Components
  alias LogflareWeb.Endpoints.RunQuery
  alias LogflareWeb.QueryErrorHelpers
  alias Logflare.Utils
  alias Phoenix.LiveView.AsyncResult

  @resource_settings [
    max_bytes_to_read: "Max bytes to read",
    max_rows_to_read: "Max rows to read",
    max_memory_usage: "Max memory usage (bytes)",
    max_execution_time: "Max execution time (seconds)"
  ]

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

    admin? = Endpoints.enforced_settings_admin?(socket.assigns.team_user || user)
    allow_access = Enum.any?([Utils.flag("endpointsOpenBeta"), user.endpoints_beta, admin?])

    alerts = Endpoints.list_endpoints_by(user_id: user.id)

    socket =
      socket
      |> assign(:user_id, user.id)
      #  must be below user_id assign
      |> refresh_endpoints()
      |> assign(:allow_access, allow_access)
      |> assign(:admin?, admin?)
      |> assign(:resource_settings, @resource_settings)
      |> assign(:alerts, alerts)
      |> assign_sources()
      |> assign_backends()

    {:ok, socket}
  end

  def handle_params(params, _uri, socket) do
    socket =
      socket
      |> cancel_query_plan()
      |> assign(
        show_endpoint: nil,
        endpoint_changeset: nil,
        parsed_result: nil,
        params_form: test_form(),
        declared_params: [],
        test_result: nil,
        can_edit_endpoint?: false,
        enforced_settings_form: nil,
        enforced_settings_error: nil
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

    accessible_endpoint = Endpoints.get_endpoint_query_by_user_access(user, id)

    endpoint =
      accessible_endpoint ||
        if(action == :edit and socket.assigns.admin?, do: Endpoints.get_endpoint_query(id))

    case endpoint do
      nil ->
        socket

      endpoint ->
        {endpoints, alerts} =
          if accessible_endpoint do
            {socket.assigns.endpoints, socket.assigns.alerts}
          else
            {Endpoints.list_endpoints_by(user_id: endpoint.user_id),
             Logflare.Alerting.list_alert_queries_by_user_id(endpoint.user_id)}
          end

        {:ok, parsed_result} =
          Endpoints.parse_query_string(
            endpoint.language,
            endpoint.query,
            Enum.filter(endpoints, &(&1.id != endpoint.id)),
            alerts
          )

        socket
        |> assign(:can_edit_endpoint?, not is_nil(accessible_endpoint))
        |> assign(:show_endpoint, endpoint)
        |> assign(
          :enforced_settings_form,
          enforced_settings_form(endpoint.enforced_clickhouse_settings)
        )
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
      {:error, {:saved, endpoint, :cache_invalidation_failed}} ->
        {:noreply,
         socket
         |> assign(:show_endpoint, endpoint)
         |> assign(:endpoint_changeset, Endpoints.change_query(endpoint))
         |> assign(:test_result, nil)
         |> put_flash(:error, "Changes saved, but cached results could not be invalidated.")}

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
        "save-enforced-settings",
        %{"settings" => params},
        %{assigns: %{user: user, show_endpoint: %EndpointQuery{} = endpoint}} = socket
      )
      when is_map(params) do
    settings = parse_enforced_settings(params)

    actor = socket.assigns.team_user || user

    case Endpoints.configure_enforced_clickhouse_settings(
           actor,
           endpoint,
           settings,
           enforced_settings_save_opts(endpoint)
         ) do
      {:ok, updated} ->
        {:noreply,
         socket
         |> assign_enforced_settings(updated)
         |> put_flash(:info, "Enforced ClickHouse settings saved")}

      {:error, {:settings_saved, updated, :cache_invalidation_failed}} ->
        {:noreply,
         socket
         |> assign_enforced_settings(updated)
         |> put_flash(
           :error,
           "Settings saved, but cache invalidation is incomplete. Retry the save."
         )}

      {:error, :forbidden} ->
        {:noreply, put_flash(socket, :error, "Not authorized to configure ClickHouse settings")}

      {:error, reason} ->
        {:noreply,
         socket
         |> assign(:enforced_settings_form, enforced_settings_form(params))
         |> assign(:enforced_settings_error, settings_error(reason))}
    end
  end

  def handle_event("save-enforced-settings", _params, socket) do
    {:noreply, put_flash(socket, :error, "Endpoint not found")}
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
    if can_run_query?(socket) do
      payload = Map.put_new(payload, "query", socket.assigns.params_form[:query].value)
      socket = socket |> cancel_query_plan() |> assign(:params_form, test_form(payload))

      socket =
        if sandbox_query?(socket, payload) do
          run_sandbox_query(socket, payload)
        else
          run_endpoint_query(socket, payload)
        end

      {:noreply, socket}
    else
      {:noreply, put_flash(socket, :error, "Not authorized to run this endpoint")}
    end
  end

  def handle_event("load-query-plan", _params, socket) do
    {:noreply, load_query_plan(socket)}
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

  def handle_event("validate", %{"run" => payload}, socket) do
    values = Map.merge(socket.assigns.params_form.params, payload)
    {:noreply, assign(socket, :params_form, test_form(values))}
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

  def handle_async({:query_plan, run_id}, outcome, socket) do
    result = socket.assigns.test_result

    socket =
      with %{run_id: ^run_id} <- result,
           {:ok, {:ok, text}} <- outcome do
        assign(socket, :test_result, %{result | query_plan: AsyncResult.ok(text)})
      else
        {:ok, {:error, error}} ->
          plan = AsyncResult.failed(result.query_plan, error)
          assign(socket, :test_result, %{result | query_plan: plan})

        {:exit, _reason} ->
          error = %QueryError{
            kind: :backend_error,
            raw_error: nil,
            backend: Adaptor.get_adaptor(result.query_plan_request.backend)
          }

          plan = AsyncResult.failed(result.query_plan, error)
          assign(socket, :test_result, %{result | query_plan: plan})

        _ ->
          socket
      end

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

  @spec can_run_query?(Phoenix.LiveView.Socket.t()) :: boolean()
  defp can_run_query?(%{assigns: %{live_action: :new}}), do: true

  defp can_run_query?(%{assigns: %{show_endpoint: %EndpointQuery{id: id}}} = socket) do
    actor = socket.assigns.team_user || socket.assigns.user
    not is_nil(Endpoints.get_endpoint_query_by_user_access(actor, id))
  end

  defp can_run_query?(_socket), do: false

  defp run_endpoint_query(
         %{assigns: %{user: %User{} = user}} = socket,
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
           endpoint_id: socket.assigns.endpoint_changeset.data.id,
           use_query_cache: false,
           redact_pii: redact_pii,
           backend_id: backend_id,
           reservation: reservation,
           prepare_query_plan: true
         ) do
      {:ok, result} ->
        socket
        |> put_flash(:info, "Ran query successfully")
        |> assign_successful_test_result(:endpoint, result, nil)

      {:error, error} ->
        message = endpoint_query_error_message(error)
        assign(socket, :test_result, %{kind: :endpoint, status: :error, error: message})
    end
  end

  @spec endpoint_query_error_message(term()) :: String.t()
  defp endpoint_query_error_message(error)
       when error in [:database_unavailable, :policy_unavailable],
       do: "Endpoint policy is temporarily unavailable. Retry the query."

  defp endpoint_query_error_message(%QueryError{} = error),
    do: QueryErrorHelpers.query_error_message(error)

  defp endpoint_query_error_message(message) when is_binary(message), do: message
  defp endpoint_query_error_message(_error), do: QueryErrorHelpers.generic_query_error_message()

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

    case Endpoints.run_query(endpoint, sandbox_params,
           reservation: reservation,
           prepare_query_plan: true
         ) do
      {:ok, result} ->
        transformed_query = maybe_transformed_query(show_transformed?, endpoint, sandbox_params)

        socket
        |> put_flash(:info, "Ran sandbox query successfully")
        |> assign_successful_test_result(:sandboxed_endpoint, result, transformed_query)

      {:error, error} ->
        Logger.error(
          "Sandbox query failed: '#{inspect(error)}', endpoint_id: #{endpoint.id}, backend_id: #{endpoint.backend_id}, sandbox_params: '#{inspect(sandbox_params)}'"
        )

        socket
        |> put_flash(:error, "Error occurred when running sandbox query")
        |> assign(:test_result, %{
          kind: :sandboxed_endpoint,
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

  defp assign_successful_test_result(socket, kind, %{rows: rows} = result, transformed_query) do
    request = Map.get(result, :query_plan_request)

    test_result = %{
      kind: kind,
      status: :ok,
      rows: rows,
      total_bytes_processed: Map.get(result, :total_bytes_processed),
      run_id: System.unique_integer([:positive]),
      query_plan_request: request,
      query_plan: if(request, do: %AsyncResult{}),
      transformed_query: transformed_query
    }

    assign(socket, :test_result, test_result)
  end

  defp load_query_plan(
         %{
           assigns: %{
             test_result:
               %{query_plan: %AsyncResult{ok?: false, loading: nil, failed: nil}} = result
           }
         } = socket
       ) do
    request = result.query_plan_request

    socket
    |> assign(:test_result, %{result | query_plan: AsyncResult.loading()})
    |> start_async({:query_plan, result.run_id}, fn -> Endpoints.explain_query(request) end)
  end

  defp load_query_plan(socket), do: socket

  defp cancel_query_plan(socket) do
    case socket.assigns[:test_result] do
      %{run_id: run_id} -> cancel_async(socket, {:query_plan, run_id})
      _ -> socket
    end
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

  @spec assign_enforced_settings(Phoenix.LiveView.Socket.t(), EndpointQuery.t()) ::
          Phoenix.LiveView.Socket.t()
  defp assign_enforced_settings(socket, endpoint) do
    socket
    |> assign(:show_endpoint, endpoint)
    |> assign(
      :enforced_settings_form,
      enforced_settings_form(endpoint.enforced_clickhouse_settings)
    )
    |> assign(:enforced_settings_error, nil)
  end

  @spec enforced_settings_form(map()) :: Phoenix.HTML.Form.t()
  defp enforced_settings_form(settings) do
    values =
      Map.new(@resource_settings, fn {key, _label} ->
        {Atom.to_string(key), Map.get(settings, Atom.to_string(key), "")}
      end)

    to_form(values, as: :settings)
  end

  @spec enforced_settings_save_opts(EndpointQuery.t()) :: keyword()
  defp enforced_settings_save_opts(%EndpointQuery{language: :ch_sql}) do
    keys = Enum.map(@resource_settings, fn {key, _label} -> Atom.to_string(key) end)
    [replace_keys: keys ++ ~w(read_overflow_mode timeout_overflow_mode)]
  end

  defp enforced_settings_save_opts(_endpoint), do: []

  @spec parse_enforced_settings(map()) :: map()
  defp parse_enforced_settings(params) do
    @resource_settings
    |> Enum.reduce(%{}, fn {key, _label}, settings ->
      name = Atom.to_string(key)

      value = Map.get(params, name)
      if value in [nil, ""], do: settings, else: Map.put(settings, name, parse_limit(key, value))
    end)
  end

  @spec parse_limit(atom(), term()) :: term()
  defp parse_limit(key, value) when is_binary(value) do
    value = String.trim(value)

    case Integer.parse(value) do
      {number, ""} -> number
      _ -> parse_fractional_limit(key, value)
    end
  end

  defp parse_limit(_key, value), do: value

  @spec parse_fractional_limit(atom(), String.t()) :: number() | String.t()
  defp parse_fractional_limit(:max_execution_time, value) do
    case Float.parse(value) do
      {number, ""} -> number
      _ -> value
    end
  end

  defp parse_fractional_limit(_key, value), do: value

  @spec settings_error(term()) :: String.t()
  defp settings_error(reason) when is_binary(reason), do: reason
  defp settings_error(_reason), do: "Unable to configure ClickHouse settings"

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
