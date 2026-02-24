defmodule LogflareWeb.BackendsLive do
  @moduledoc false
  use LogflareWeb, :live_view

  import LogflareWeb.Utils, only: [stringify_changeset_errors: 1]

  alias Logflare.Backends
  alias Logflare.Rules
  alias Logflare.Sources

  require Logger

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
      |> assign(:show_rule_form?, false)
      |> assign(:show_alert_form?, false)
      |> assign(:alert_options, [])
      |> assign(:form_type, nil)
      |> assign(:show_default_ingest_form?, false)
      |> assign(:default_ingest_sources, [])
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
    params = transform_params(params)

    socket =
      case Backends.update_backend(socket.assigns.backend, params) do
        {:ok, backend} ->
          socket
          |> assign(:show_rule_form?, false)
          |> refresh_backend(backend.id)
          |> refresh_backends()
          |> put_flash(:info, "Successfully updated backend")
          |> push_patch(to: ~p"/backends/#{backend.id}")

        {:error, changeset} ->
          message = stringify_changeset_errors(changeset)
          put_flash(socket, :error, "Encountered error when updating backend:\n#{message}")
      end

    {:noreply, socket}
  end

  def handle_event(
        "save_backend",
        %{"backend" => params},
        %{assigns: %{live_action: :new}} = socket
      ) do
    params = transform_params(params)

    socket =
      case Logflare.Backends.create_backend(params) do
        {:ok, backend} ->
          socket
          |> assign(:show_rule_form?, false)
          |> assign(:backends, [backend | socket.assigns.backends])
          |> put_flash(:info, "Successfully created backend")
          |> push_patch(to: ~p"/backends/#{backend.id}")

        {:error, changeset} ->
          message = stringify_changeset_errors(changeset)

          put_flash(socket, :error, "Encountered error when adding backend:\n#{message}")
      end

    socket = refresh_backends(socket)

    {:noreply, socket}
  end

  def handle_event("save_rule", %{"rule" => params}, socket) do
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

  def handle_event("change_form_type", %{"backend" => %{"type" => type}}, socket) do
    {:noreply, assign(socket, form_type: type)}
  end

  def handle_event("toggle_default_ingest_form", _params, socket) do
    {:noreply,
     assign(socket, :show_default_ingest_form?, !socket.assigns.show_default_ingest_form?)}
  end

  def handle_event(
        "save_default_ingest",
        %{"default_ingest" => %{"source_id" => source_id}},
        socket
      ) do
    backend = socket.assigns.backend

    socket =
      case Backends.update_backend(backend, %{default_ingest?: true, source_id: source_id}) do
        {:ok, _backend} ->
          socket
          |> refresh_backend(backend.id)
          |> assign(:show_default_ingest_form?, false)
          |> put_flash(:info, "Successfully marked backend as default ingest for source")

        {:error, changeset} ->
          message = stringify_changeset_errors(changeset)
          put_flash(socket, :error, "Error setting default ingest:\n#{message}")
      end

    {:noreply, socket}
  end

  def handle_event("add_all_default_ingest", _params, socket) do
    backend = socket.assigns.backend
    available_sources = socket.assigns.available_sources
    {:ok, _backend} = Backends.add_all_default_ingest_sources(backend, available_sources)

    socket =
      socket
      |> refresh_backend(backend.id)
      |> assign(:show_default_ingest_form?, false)
      |> put_flash(:info, "Successfully added all available sources as default ingest")

    {:noreply, socket}
  end

  def handle_event("remove_all_default_ingest", _params, socket) do
    backend = socket.assigns.backend
    {:ok, _backend} = Backends.remove_all_default_ingest_sources(backend)

    socket =
      socket
      |> refresh_backend(backend.id)
      |> put_flash(:info, "Successfully removed all default ingest sources")

    {:noreply, socket}
  end

  def handle_event("remove_default_ingest", %{"source_id" => source_id}, socket) do
    backend = socket.assigns.backend
    source = Sources.get(source_id)

    # Remove this backend from the source's backends
    updated_backends =
      source
      |> Sources.preload_backends()
      |> Map.get(:backends, [])
      |> Enum.reject(&(&1.id == backend.id))

    socket =
      case Backends.update_source_backends(source, updated_backends) do
        {:ok, _} ->
          # If no more sources are using this backend, disable default_ingest flag
          remaining_sources = Sources.list_sources(backend_id: backend.id)

          if Enum.empty?(remaining_sources) do
            Backends.update_backend(backend, %{default_ingest?: false})
          end

          socket
          |> refresh_backend(backend.id)
          |> put_flash(:info, "Removed default ingest for source")

        {:error, _} ->
          put_flash(socket, :error, "Error removing default ingest")
      end

    {:noreply, socket}
  end

  def handle_event("toggle_rule_form", _params, socket) do
    {:noreply, socket |> assign(:show_rule_form?, !socket.assigns.show_rule_form?)}
  end

  def handle_event("delete_rule", %{"rule_id" => rule_id}, socket) do
    rule = Rules.get_rule(rule_id)
    Rules.delete_rule(rule)

    {:noreply,
     socket
     |> assign(:show_rule_form?, false)
     |> refresh_backend(socket.assigns.backend.id)
     |> put_flash(:info, "Rule has been deleted successfully")}
  end

  def handle_event("delete", %{"backend_id" => id}, socket) do
    Logger.debug("Removing backend id: #{id}")
    backend = Backends.get_backend(id)

    with {:ok, _backend} <- Backends.delete_backend(backend) do
      socket =
        socket
        |> put_flash(:info, "Successfully deleted backend of type #{backend.type}")
        |> refresh_backends()
        |> push_patch(to: ~p"/backends")

      {:noreply, socket}
    else
      {:error, changeset} ->
        message = stringify_changeset_errors(changeset)

        {:noreply,
         put_flash(socket, :error, "Encountered error when adding backend:\n#{message}")}
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
    alert_query = Logflare.Alerting.get_alert_query!(alert_id)

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

  def handle_event("remove_alert", %{"alert_id" => alert_id}, socket) do
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
    socket
    |> assign(:backend_types, [
      {"Webhook", :webhook},
      {"Postgres", :postgres},
      {"BigQuery", :bigquery},
      {"Datadog", :datadog},
      {"Elastic", :elastic},
      {"Loki", :loki},
      {"ClickHouse", :clickhouse},
      {"Incident.io", :incidentio},
      {"S3", :s3},
      {"Axiom", :axiom},
      {"OTLP", :otlp},
      {"Last9", :last9}
    ])
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
    |> assign(:form_type, nil)
  end

  defp refresh_backend(socket, id) do
    backend = Backends.get_backend(id) |> Backends.preload_rules() |> Backends.preload_alerts()

    # Load sources that use this backend as default ingest
    default_ingest_sources =
      if backend && backend.default_ingest? do
        Sources.list_sources(backend_id: backend.id)
        |> Enum.sort_by(& &1.name)
      else
        []
      end

    # Calculate available sources for the dropdown (excluding already associated ones)
    available_sources =
      socket.assigns.sources
      |> Enum.filter(& &1.default_ingest_backend_enabled?)
      |> Enum.reject(fn source ->
        Enum.any?(default_ingest_sources, &(&1.id == source.id))
      end)
      |> Enum.sort_by(& &1.name)

    socket
    |> assign(:backend, backend)
    |> assign(:form_type, Atom.to_string(backend.type))
    |> assign(:default_ingest_sources, default_ingest_sources)
    |> assign(:available_sources, available_sources)
  end

  defp transform_params(params) do
    type = params["type"]

    Map.update(params, "config", nil, fn config ->
      headers_form_keys =
        for i <- 1..2 do
          ["header#{i}_key", "header#{i}_value"]
        end

      {headers, config} = Map.split(config, List.flatten(headers_form_keys))

      headers =
        for [form_key, form_value] <- headers_form_keys,
            key = headers[form_key],
            key != "",
            value = headers[form_value],
            into: %{} do
          {key, value}
        end

      Map.put(config, "headers", headers)
      |> case do
        %{"metadata" => metadata_str} = config
        when is_binary(metadata_str) and type == "incidentio" ->
          metadata = parse_incidentio_metadata(metadata_str)
          Map.put(config, "metadata", metadata)

        config ->
          config
      end
    end)
  end

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
end
