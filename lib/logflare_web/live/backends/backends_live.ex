defmodule LogflareWeb.BackendsLive do
  @moduledoc false

  use LogflareWeb, :live_view

  import LogflareWeb.Backends.Components
  import LogflareWeb.Utils, only: [stringify_changeset_errors: 1, with_team_param: 2]

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.HttpBased.Headers
  alias Logflare.Backends.Adaptor.WebhookAdaptor
  alias Logflare.Backends.Backend
  alias Logflare.Rules
  alias Logflare.Sources
  alias LogflareWeb.Backends.ReadClusterUrlsComponent
  alias Logflare.Backends.Adaptor.ElasticAdaptor

  require Logger

  @header_form_key_regex ~r/^header(\d+)_(stored_key|key|value)$/

  embed_templates("actions/*", suffix: "_action")
  embed_templates("components/*")

  def render(%{live_action: :index} = assigns), do: index_action(assigns)
  def render(%{live_action: :show} = assigns), do: show_action(assigns)
  def render(%{live_action: :new} = assigns), do: new_action(assigns)
  def render(%{live_action: :edit} = assigns), do: edit_action(assigns)

  def mount(params, _session, socket) do
    %{assigns: %{user: user}} = socket

    socket =
      socket
      |> assign(:user, user)
      |> assign(:backends, [])
      |> assign(:backend, nil)
      |> assign(:backend_changeset, nil)
      |> assign(:sources, Sources.list_sources_by_user(user.id))
      |> assign(:connection_status, nil)
      |> assign(:show_rule_form?, false)
      |> assign(:show_alert_form?, false)
      |> assign(:alert_options, [])
      |> assign(:form_type, nil)
      |> assign(:elastic_transport, "filebeat")
      |> assign(:default_ingest_capable?, false)
      |> assign(:opted_in_count, 0)
      |> assign(:eligible_count, 0)
      |> assign(:flag_multibackend, Logflare.Utils.flag("multibackend", user))
      |> assign_backend_types()
      |> refresh_backends()
      |> refresh_backend(params["id"])

    verify_resource_access(socket)
    {:ok, socket, layout: {LogflareWeb.LayoutView, :inline_live}}
  end

  def handle_params(params, _uri, socket) do
    socket =
      socket
      |> refresh_backends()
      |> refresh_backend(params["id"])

    verify_resource_access(socket)

    {:noreply, socket}
  end

  defp verify_resource_access(%{assigns: %{user: user, backend: backend}}) when backend != nil do
    if backend.user_id != user.id do
      raise LogflareWeb.ErrorsLive.InvalidResourceError
    end
  end

  defp verify_resource_access(_socket), do: :ok

  def handle_event(
        "save_backend",
        %{"backend" => params},
        %{assigns: %{live_action: :edit}} = socket
      ) do
    with {:ok, params} <- transform_params(params, existing_headers(socket.assigns.backend)) do
      socket =
        case Backends.update_backend(socket.assigns.backend, params) do
          {:ok, backend} ->
            socket
            |> assign(:show_rule_form?, false)
            |> refresh_backend(backend.id)
            |> refresh_backends()
            |> put_flash(:info, "Successfully updated backend")
            |> push_patch(to: with_team_param(~p"/backends/#{backend.id}", socket.assigns.team))

          {:error, changeset} ->
            message = stringify_changeset_errors(changeset)
            put_flash(socket, :error, "Encountered error when updating backend:\n#{message}")
        end

      {:noreply, socket}
    else
      {:error, message} -> {:noreply, put_flash(socket, :error, message)}
    end
  end

  def handle_event(
        "save_backend",
        %{"backend" => params},
        %{assigns: %{live_action: :new}} = socket
      ) do
    with {:ok, params} <- transform_params(params) do
      socket =
        case Logflare.Backends.create_backend(socket.assigns.user, params) do
          {:ok, backend} ->
            socket
            |> assign(:show_rule_form?, false)
            |> assign(:backends, [backend | socket.assigns.backends])
            |> put_flash(:info, "Successfully created backend")
            |> push_patch(to: with_team_param(~p"/backends/#{backend.id}", socket.assigns.team))

          {:error, changeset} ->
            message = stringify_changeset_errors(changeset)

            put_flash(socket, :error, "Encountered error when adding backend:\n#{message}")
        end

      socket = refresh_backends(socket)

      {:noreply, socket}
    else
      {:error, message} -> {:noreply, put_flash(socket, :error, message)}
    end
  end

  def handle_event("save_rule", %{"rule" => params}, socket) do
    %{assigns: %{user: user}} = socket

    source = params["source_id"] && Sources.get_by_user_access(user, params["source_id"])

    backend =
      params["backend_id"] && Backends.get_backend_by_user_access(user, params["backend_id"])

    cond do
      source == nil ->
        {:noreply, put_flash(socket, :error, "You do not have access to that source.")}

      backend == nil ->
        {:noreply, put_flash(socket, :error, "You do not have access to that backend.")}

      true ->
        do_save_rule(socket, params)
    end
  end

  def handle_event("change_form_type", %{"backend" => %{"type" => type}}, socket) do
    socket =
      socket
      |> assign(:form_type, type)
      |> then(fn s ->
        if type == "elastic" do
          assign(s, :elastic_transport, "filebeat")
        else
          s
        end
      end)

    {:noreply, socket}
  end

  def handle_event(
        "change_elastic_transport",
        %{"backend" => %{"config" => %{"transport" => transport}}},
        socket
      ) do
    {:noreply, assign(socket, :elastic_transport, transport)}
  end

  def handle_event(
        "test_connection",
        _params,
        %{assigns: %{connection_status: %{loading: true}}} = socket
      ) do
    {:noreply, socket}
  end

  def handle_event("test_connection", _params, socket) do
    backend = socket.assigns.backend

    {:noreply,
     assign_async(
       socket,
       :connection_status,
       fn ->
         with :ok <- Backends.test_connection(backend) do
           {:ok, %{connection_status: :ok}}
         end
       end,
       reset: true
     )}
  end

  def handle_event("toggle_backend", %{"backend_id" => backend_id}, socket) do
    effective_user = socket.assigns[:team_user] || socket.assigns.user

    socket =
      if backend = Backends.get_backend_by_user_access(effective_user, backend_id) do
        toggle_backend(socket, backend)
      else
        put_flash(socket, :error, "You do not have access to that backend.")
      end

    {:noreply, socket}
  end

  def handle_event("enable_default_ingest", _params, socket) do
    {:noreply, set_default_ingest(socket, true)}
  end

  def handle_event("disable_default_ingest", _params, socket) do
    {:noreply, set_default_ingest(socket, false)}
  end

  def handle_event("opt_in_all_sources", _params, socket) do
    {:noreply, set_opt_in_for_all(socket, true)}
  end

  def handle_event("opt_out_all_sources", _params, socket) do
    {:noreply, set_opt_in_for_all(socket, false)}
  end

  def handle_event("toggle_rule_form", _params, socket) do
    {:noreply, socket |> assign(:show_rule_form?, !socket.assigns.show_rule_form?)}
  end

  def handle_event("delete_rule", %{"rule_id" => rule_id}, socket) do
    %{assigns: %{user: user, backend: current_backend}} = socket
    rule = Rules.get_rule(rule_id)

    cond do
      current_backend == nil or current_backend.user_id != user.id ->
        {:noreply, put_flash(socket, :error, "You do not have access to this backend.")}

      rule == nil or rule.backend_id != current_backend.id ->
        {:noreply, put_flash(socket, :error, "Rule not found on this backend.")}

      true ->
        Rules.delete_rule(rule)

        {:noreply,
         socket
         |> assign(:show_rule_form?, false)
         |> refresh_backend(current_backend.id)
         |> put_flash(:info, "Rule has been deleted successfully")}
    end
  end

  def handle_event("delete", %{"backend_id" => id}, socket) do
    Logger.debug("Removing backend id: #{id}")
    backend = Backends.get_backend_by_user_access(socket.assigns.user, id)

    if backend == nil do
      {:noreply, put_flash(socket, :error, "You do not have access to that backend.")}
    else
      delete_backend(socket, backend)
    end
  end

  def handle_event("toggle_alert_form", _params, socket) do
    socket =
      if socket.assigns.show_alert_form? do
        assign(socket, :show_alert_form?, false)
      else
        # Load alert options when form is toggled open
        alert_queries = Logflare.Alerting.list_alert_queries_by_user_id(socket.assigns.user.id)
        alert_options = Enum.map(alert_queries, fn alert -> {alert.name, alert.id} end)

        socket
        |> assign(:alert_options, alert_options)
        |> assign(:show_alert_form?, true)
      end

    {:noreply, socket}
  end

  def handle_event("add_alert", %{"alert" => %{"alert_id" => alert_id}}, socket) do
    %{assigns: %{user: user, backend: backend}} = socket

    if backend == nil or backend.user_id != user.id do
      {:noreply, put_flash(socket, :error, "You do not have access to this backend.")}
    else
      case Logflare.Alerting.get_alert_query_by_user_access(user, alert_id) do
        nil ->
          {:noreply, put_flash(socket, :error, "You do not have access to that alert.")}

        alert_query ->
          do_add_alert(socket, alert_query)
      end
    end
  end

  def handle_event("remove_alert", %{"alert_id" => alert_id}, socket) do
    %{assigns: %{user: user, backend: backend}} = socket

    if backend == nil or backend.user_id != user.id do
      {:noreply, put_flash(socket, :error, "You do not have access to this backend.")}
    else
      do_remove_alert(socket, alert_id)
    end
  end

  @spec set_default_ingest(Phoenix.LiveView.Socket.t(), boolean()) :: Phoenix.LiveView.Socket.t()
  defp set_default_ingest(%{assigns: %{backend: backend}} = socket, enabled?) do
    case Backends.update_backend(backend, %{default_ingest?: enabled?}) do
      {:ok, _backend} ->
        socket
        |> refresh_backend(backend.id)
        |> put_flash(:info, default_ingest_flash(enabled?))

      {:error, changeset} ->
        message = stringify_changeset_errors(changeset)
        put_flash(socket, :error, "Error updating default ingest:\n#{message}")
    end
  end

  defp default_ingest_flash(true), do: "This backend is now the account default ingest backend"
  defp default_ingest_flash(false), do: "Default ingest disabled for this backend"

  @spec set_opt_in_for_all(Phoenix.LiveView.Socket.t(), boolean()) :: Phoenix.LiveView.Socket.t()
  defp set_opt_in_for_all(%{assigns: %{backend: backend}} = socket, opt_in?) do
    {:ok, count} = Sources.set_default_ingest_opt_in(backend.user_id, opt_in?)

    socket
    |> refresh_backend(backend.id)
    |> put_flash(:info, "#{count} sources #{opt_in_verb(opt_in?)}")
  end

  defp opt_in_verb(true), do: "opted in"
  defp opt_in_verb(false), do: "opted out"

  defp do_remove_alert(socket, alert_id) do
    alert_id = String.to_integer(alert_id)

    alert_queries =
      socket.assigns.backend.alert_queries
      |> Enum.reject(&(&1.id == alert_id))

    socket =
      case Logflare.Backends.update_backend(socket.assigns.backend, %{
             alert_queries: alert_queries
           }) do
        {:ok, _backend} ->
          socket
          |> refresh_backend(socket.assigns.backend.id)
          |> put_flash(:info, "Alert successfully removed from backend")

        {:error, changeset} ->
          message = stringify_changeset_errors(changeset)
          put_flash(socket, :error, "Encountered error when removing alert:\n#{message}")
      end

    {:noreply, socket}
  end

  defp assign_backend_types(socket) do
    assign(socket, :backend_types, Backend.types_with_labels())
  end

  defp refresh_backends(socket) do
    backends =
      Backends.list_backends_by_user_id(socket.assigns.user.id)
      |> Backends.preload_rules()

    socket
    |> assign(:backends, backends)
  end

  defp refresh_backend(socket, nil) do
    socket
    |> assign(:backend, nil)
    |> assign(:connection_status, nil)
    |> assign(:form_type, nil)
    |> assign(:elastic_transport, "filebeat")
  end

  defp refresh_backend(socket, id) do
    case Backends.get_backend_by_user_access(socket.assigns.user, id) do
      nil ->
        raise LogflareWeb.ErrorsLive.InvalidResourceError

      backend ->
        backend = backend |> Backends.preload_rules() |> Backends.preload_alerts()
        do_refresh_backend(socket, backend)
    end
  end

  defp maybe_refresh_current_backend(socket, id) do
    if socket.assigns.backend && socket.assigns.backend.id == id do
      refresh_backend(socket, id)
    else
      socket
    end
  end

  defp toggle_backend(socket, backend) do
    case Backends.update_backend(backend, %{enabled: !backend.enabled}) do
      {:ok, updated} ->
        socket
        |> refresh_backends()
        |> maybe_refresh_current_backend(updated.id)
        |> put_flash(
          :info,
          "Destination #{if(updated.enabled, do: "enabled", else: "disabled")}"
        )

      {:error, changeset} ->
        message = stringify_changeset_errors(changeset)
        put_flash(socket, :error, "Unable to update destination:\n#{message}")
    end
  end

  defp do_refresh_backend(socket, backend) do
    capable? = Adaptor.default_ingest_capable?(backend)

    %{eligible: eligible_count, opted_in: opted_in_count} =
      default_ingest_counts(backend, capable?)

    elastic_transport =
      if backend.type == :elastic do
        ElasticAdaptor.transport(backend.config || %{})
      else
        "filebeat"
      end

    socket
    |> assign(:backend, backend)
    |> assign(:connection_status, nil)
    |> assign(:form_type, Atom.to_string(backend.type))
    |> assign(:elastic_transport, elastic_transport)
    |> assign(:default_ingest_capable?, capable?)
    |> assign(:opted_in_count, opted_in_count)
    |> assign(:eligible_count, eligible_count)
  end

  defp default_ingest_counts(%Backend{user_id: user_id, default_ingest?: true}, true),
    do: Sources.count_default_ingest_opt_in(user_id)

  defp default_ingest_counts(_backend, _capable?), do: %{eligible: 0, opted_in: 0}

  @spec read_cluster_component_id(Backend.t() | nil) :: String.t()
  defp read_cluster_component_id(%Backend{id: id}), do: "read-cluster-urls-#{id}"
  defp read_cluster_component_id(_backend), do: "read-cluster-urls-new"

  @spec transform_params(map(), map()) :: {:ok, map()} | {:error, String.t()}
  defp transform_params(params, existing_headers \\ %{}) do
    type = params["type"]

    params
    |> Map.update("config", nil, fn config ->
      {header_params, config} = Map.split(config, header_form_keys(config))

      config =
        if map_size(header_params) == 0 do
          config
        else
          Map.put(config, "headers", build_headers(header_params, existing_headers))
        end

      transform_config_for_type(config, type)
    end)
    |> assemble_read_clusters()
  end

  @spec existing_headers(Backend.t() | nil) :: map()
  defp existing_headers(%Backend{config: config}) when is_map(config) do
    Headers.normalize_keys(Map.get(config, :headers) || %{})
  end

  defp existing_headers(_backend), do: %{}

  @spec header_form_keys(map()) :: [String.t()]
  defp header_form_keys(config) when is_map(config) do
    for key <- Map.keys(config), Regex.match?(@header_form_key_regex, key), do: key
  end

  defp header_form_keys(_config), do: []

  @spec build_headers(map(), map()) :: map()
  defp build_headers(header_params, existing_headers) do
    for index <- header_form_indexes(header_params),
        key = header_params["header#{index}_key"],
        is_binary(key),
        key != "",
        into: %{} do
      {key, header_value(header_params, index, existing_headers)}
    end
  end

  # Restores the stored secret for a row whose value input still holds the redaction
  # sentinel. The lookup uses the key the row was rendered with, so renaming a key
  # carries its stored value over instead of writing the sentinel or dropping the
  # header. Rows the user actually edited pass through untouched.
  @spec header_value(map(), String.t(), map()) :: term()
  defp header_value(header_params, index, existing_headers) do
    value = header_params["header#{index}_value"]
    stored_key = header_params["header#{index}_stored_key"]

    with true <- value == WebhookAdaptor.redacted_value(),
         true <- is_binary(stored_key) and stored_key != "",
         {:ok, stored_value} <- Map.fetch(existing_headers, Headers.normalize_key(stored_key)) do
      stored_value
    else
      _ -> value
    end
  end

  @spec header_form_indexes(map()) :: [String.t()]
  defp header_form_indexes(header_params) do
    header_params
    |> Map.keys()
    |> Enum.map(&Regex.run(@header_form_key_regex, &1, capture: :all_but_first))
    |> Enum.map(fn [index, _field] -> index end)
    |> Enum.uniq()
  end

  @spec assemble_read_clusters(map()) :: {:ok, map()} | {:error, String.t()}
  defp assemble_read_clusters(%{"config" => config} = params) when is_map(config) do
    if has_read_cluster_fields?(config) do
      with {:ok, config} <- ReadClusterUrlsComponent.assemble_read_only_urls(config) do
        {:ok, %{params | "config" => config}}
      end
    else
      {:ok, params}
    end
  end

  defp assemble_read_clusters(params), do: {:ok, params}

  @spec has_read_cluster_fields?(map()) :: boolean()
  defp has_read_cluster_fields?(config) do
    Enum.any?(config, fn {key, _value} -> String.starts_with?(key, "read_cluster_label_") end)
  end

  defp transform_config_for_type(%{"metadata" => metadata_str} = config, "incidentio")
       when is_binary(metadata_str) do
    Map.put(config, "metadata", parse_incidentio_metadata(metadata_str))
  end

  defp transform_config_for_type(config, _type), do: config

  defp parse_incidentio_metadata(data) when is_binary(data) do
    data
    |> String.split(",")
    |> Enum.reduce(%{}, fn pair, acc ->
      case String.split(pair, "=", parts: 2) do
        [key, value] -> Map.put(acc, String.trim(key), String.trim(value))
        _ -> acc
      end
    end)
  end

  defp do_save_rule(socket, params) do
    socket =
      case Rules.create_rule(params) do
        {:ok, _rule} ->
          socket
          |> refresh_backend(socket.assigns.backend.id)
          |> assign(:show_rule_form?, false)
          |> put_flash(:info, "Successfully created rule for #{socket.assigns.backend.name}")

        {:error, changeset} ->
          message = stringify_changeset_errors(changeset)

          put_flash(socket, :error, "Encountered error when adding rule:\n#{message}")
      end

    socket = refresh_backends(socket)

    {:noreply, socket}
  end

  defp do_add_alert(socket, alert_query) do
    socket =
      case Logflare.Backends.update_backend(socket.assigns.backend, %{
             alert_queries: [alert_query | socket.assigns.backend.alert_queries]
           }) do
        {:ok, _backend} ->
          socket
          |> assign(:show_alert_form?, false)
          |> refresh_backend(socket.assigns.backend.id)
          |> put_flash(:info, "Alert successfully added to backend")

        {:error, changeset} ->
          message = stringify_changeset_errors(changeset)
          put_flash(socket, :error, "Encountered error when adding alert:\n#{message}")
      end

    {:noreply, socket}
  end

  defp delete_backend(socket, backend) do
    with {:ok, _backend} <- Backends.delete_backend(backend) do
      socket =
        socket
        |> put_flash(:info, "Successfully deleted backend of type #{backend.type}")
        |> refresh_backends()
        |> push_patch(to: with_team_param(~p"/backends", socket.assigns.team))

      {:noreply, socket}
    else
      {:error, changeset} ->
        message = stringify_changeset_errors(changeset)

        {:noreply,
         put_flash(socket, :error, "Encountered error when adding backend:\n#{message}")}
    end
  end
end
