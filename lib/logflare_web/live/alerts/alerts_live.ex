defmodule LogflareWeb.AlertsLive do
  @moduledoc false
  use LogflareWeb, :live_view
  use Phoenix.Component

  import Ecto.Query
  import LogflareWeb.Utils, only: [stringify_changeset_errors: 2]

  alias Logflare.Alerting
  alias Logflare.Alerting.AlertQuery
  alias Logflare.Backends
  alias Logflare.Endpoints
  alias Logflare.Repo
  alias LogflareWeb.AuthLive
  alias LogflareWeb.QueryComponents
  alias LogflareWeb.Utils

  require Logger

  @past_jobs_page_size 15
  @poll_interval 500
  @poll_max_attempts 60

  embed_templates("actions/*", suffix: "_action")
  embed_templates("components/*")

  def render(%{live_action: :index} = assigns), do: index_action(assigns)
  def render(%{live_action: :show, alert: nil} = assigns), do: not_found_action(assigns)
  def render(%{live_action: :show} = assigns), do: show_action(assigns)
  def render(%{live_action: :new} = assigns), do: new_action(assigns)
  def render(%{live_action: :edit} = assigns), do: edit_action(assigns)

  defp render_docs_link(assigns) do
    ~H"""
    <.subheader_link to="https://docs.logflare.app/alerts" external={true} text="docs" fa_icon="book" />
    """
  end

  def mount(%{}, _session, socket) do
    %{assigns: %{user: user}} = socket

    socket =
      socket
      |> assign(:user_id, user.id)
      #  must be below user_id assign
      |> refresh()
      |> assign(:query_result_rows, nil)
      |> assign(:total_bytes_processed, nil)
      |> assign(:alert, nil)
      |> assign(:future_jobs, [])
      |> assign(:past_jobs_page, nil)
      # to be lazy loaded
      |> assign(:backend_options, [])
      |> assign(:changeset, Alerting.change_alert_query(%AlertQuery{}))
      |> assign(:base_url, LogflareWeb.Endpoint.url())
      |> assign(:parse_error_message, nil)
      |> assign(:query_string, nil)
      |> assign(:show_add_backend_form, false)
      |> assign(:show_execution_history, false)
      |> assign(:triggering_alert, false)
      |> assign(:pending_job_id, nil)
      |> assign(:modal_results, nil)
      |> assign(:modal_job_id, nil)
      |> assign(:modal_node, nil)
      |> assign_endpoints_and_sources()

    {:ok, socket}
  end

  def handle_params(params, _uri, %{assigns: %{live_action: :new}} = socket) do
    {:ok, formatted_query} =
      Map.get(params, "query", "")
      |> SqlFmt.format_query()

    params = Map.put(params, "query", formatted_query)

    changeset =
      Alerting.change_alert_query(%AlertQuery{}, params)

    verify_resource_access(socket)
    {:noreply, assign(socket, :changeset, changeset)}
  end

  def handle_params(%{"id" => id} = params, _uri, socket) do
    socket =
      with user <- socket.assigns.team_user || socket.assigns.user,
           alert when is_struct(alert) <- Alerting.get_alert_query_by_user_access(user, id),
           alert <- Alerting.preload_alert_query(alert),
           alert <- Repo.preload(alert, user: :team) do
        page_num = String.to_integer(params["page"] || "1")

        socket
        |> assign(:alert, alert)
        |> assign(:future_jobs, Alerting.list_future_jobs(alert.id))
        |> assign(:past_jobs_page, paginate_past_jobs(alert.id, page_num))
        |> assign(:changeset, Alerting.change_alert_query(alert))
        |> AuthLive.assign_context_by_resource(alert, user.email)
      else
        nil ->
          socket
          |> put_flash(:info, "Alert not found!")
          |> push_navigate(to: ~p"/alerts" |> Utils.with_team_param(socket.assigns.team))
      end

    {:noreply, socket}
  end

  def handle_params(_params, _uri, socket) do
    {:noreply, socket}
  end

  defp verify_resource_access(%{assigns: %{user: user, alert: alert}}) when alert != nil do
    if alert.user_id != user.id do
      raise LogflareWeb.ErrorsLive.InvalidResourceError
    end
  end

  defp verify_resource_access(_socket), do: :ok

  def handle_event(
        "save",
        %{"alert" => params},
        %{assigns: %{user: user, alert: alert}} = socket
      ) do
    Logger.debug("Saving alert", params: params)

    case upsert_alert(alert, user, params) do
      {:ok, updated_alert} ->
        verb = if alert, do: "updated", else: "created"

        {:noreply,
         socket
         |> assign(:alert, updated_alert |> Alerting.preload_alert_query())
         |> put_flash(:info, "Successfully #{verb} alert #{updated_alert.name}")
         |> push_patch(
           to: ~p"/alerts/#{updated_alert.id}" |> Utils.with_team_param(socket.assigns.team)
         )}

      {:error, %Ecto.Changeset{} = changeset} ->
        verb = if alert, do: "update", else: "create"

        message = "Could not #{verb} alert. Please fix the errors before trying again."

        socket =
          socket
          |> put_flash(:info, message)
          |> assign(:changeset, changeset)

        {:noreply, socket}
    end
  end

  def handle_event(
        "delete",
        %{"alert_id" => id},
        %{assigns: _assigns} = socket
      ) do
    alert = Alerting.get_alert_query!(id)
    {:ok, _} = Alerting.delete_alert_query(alert)

    {:noreply,
     socket
     |> refresh()
     |> assign(:alert, nil)
     |> put_flash(:info, "#{alert.name} has been deleted")
     |> push_patch(to: "/alerts")}
  end

  def handle_event(
        "remove-slack",
        _params,
        %{assigns: %{alert: %_{id: alert_id}}} = socket
      ) do
    alert = Alerting.get_alert_query!(alert_id)

    with {:ok, alert} <- Alerting.update_alert_query(alert, %{slack_hook_url: nil}) do
      alert = Alerting.preload_alert_query(alert)

      {:noreply,
       socket
       |> assign(:alert, alert)
       |> put_flash(:info, "Slack notifications have been removed.")}
    else
      {:error, %Ecto.Changeset{} = changeset} ->
        error_message =
          stringify_changeset_errors(changeset, "Failed to remove Slack notifications")

        {:noreply, put_flash(socket, :error, error_message)}
    end
  end

  def handle_event("clear-results", _params, socket) do
    {:noreply,
     socket
     |> assign(:query_result_rows, nil)
     |> put_flash(:info, "Query run results has been cleared")}
  end

  def handle_event(
        "run-query",
        params,
        %{assigns: %{alert: %_{} = alert}} = socket
      ) do
    query = get_in(params, ["query"]) || socket.assigns.query_string || alert.query
    test_alert = %{alert | query: query}

    with {:ok, %{rows: [_ | _]} = result} <-
           Alerting.execute_alert_query(test_alert, use_query_cache: false) do
      {:noreply,
       socket
       |> assign(:query_result_rows, result.rows)
       |> assign(:total_bytes_processed, result.total_bytes_processed)
       |> put_flash(:info, "Query executed successfully. Alert will fire.")}
    else
      {:ok, %{rows: []} = result} ->
        {:noreply,
         socket
         |> assign(:query_result_rows, [])
         |> assign(:total_bytes_processed, result.total_bytes_processed)
         |> put_flash(:info, "No results from query. Alert will not fire.")}

      {:error, err} ->
        {:noreply,
         socket
         |> put_flash(
           :error,
           "Error when running query: #{inspect(err)}"
         )}
    end
  end

  def handle_event(
        "run-query",
        _params,
        %{assigns: %{alert: %_{} = alert}} = socket
      ) do
    with {:ok, result} <- Alerting.execute_alert_query(alert, use_query_cache: false) do
      {:noreply,
       socket
       |> assign(:query_result_rows, result.rows)
       |> assign(:total_bytes_processed, result.total_bytes_processed)
       |> put_flash(:info, "Alert has been triggered. Notifications sent!")}
    else
      {:error, :no_results} ->
        {:noreply,
         socket
         |> put_flash(
           :error,
           "Alert has been triggered. No results from query, notifications not sent!"
         )}

      {:error, err} ->
        {:noreply,
         socket
         |> put_flash(
           :error,
           "Error when running query: #{inspect(err)}"
         )}
    end
  end

  def handle_event(
        "add-backend",
        %{"backend" => %{"backend_id" => backend_id}},
        %{assigns: %{alert: alert}} = socket
      ) do
    backend = Backends.get_backend(backend_id)

    socket =
      if backend do
        case Alerting.update_alert_query(alert, %{backends: [backend | alert.backends]}) do
          {:ok, updated_alert} ->
            updated_alert = Alerting.preload_alert_query(updated_alert)

            socket
            |> assign(:alert, updated_alert)
            |> put_flash(:info, "Backend added successfully")

          {:error, %Ecto.Changeset{} = changeset} ->
            error_message = stringify_changeset_errors(changeset, "Failed to add backend")

            socket
            |> put_flash(:error, error_message)
        end
      else
        socket
        |> put_flash(:error, "Backend not found")
      end

    {:noreply, socket}
  end

  def handle_event(
        "remove-backend",
        %{"backend_id" => backend_id},
        %{assigns: %{alert: alert}} = socket
      ) do
    backend = Backends.get_backend(backend_id)

    socket =
      if backend do
        Alerting.update_alert_query(alert, %{
          backends: Enum.filter(alert.backends, &(&1.id != backend.id))
        })
        |> case do
          {:ok, updated_alert} ->
            updated_alert = Alerting.preload_alert_query(updated_alert)

            socket
            |> assign(:alert, updated_alert)
            |> put_flash(:info, "Backend removed successfully")

          {:error, %Ecto.Changeset{} = changeset} ->
            error_message = stringify_changeset_errors(changeset, "Failed to remove backend")

            socket
            |> put_flash(:error, error_message)
        end
      else
        socket
        |> put_flash(:error, "Backend not found")
      end

    {:noreply, socket}
  end

  def handle_event(
        "trigger-now",
        _params,
        %{assigns: %{alert: %_{} = alert}} = socket
      ) do
    case Alerting.trigger_alert_now(alert) do
      {:ok, job} ->
        Process.send_after(self(), {:poll_job, job.id, 0}, @poll_interval)

        {:noreply,
         socket
         |> assign(:triggering_alert, true)
         |> assign(:pending_job_id, job.id)}

      {:error, _} ->
        {:noreply, put_flash(socket, :error, "Failed to enqueue alert job.")}
    end
  end

  def handle_event("toggle-add-backend", _params, socket) do
    socket =
      if socket.assigns.show_add_backend_form do
        socket
      else
        backends = Backends.list_backends(user_id: socket.assigns.user_id, types: [:incidentio])
        backend_options = Enum.map(backends, fn b -> {b.name, b.id} end)
        assign(socket, :backend_options, backend_options)
      end

    {:noreply, assign(socket, :show_add_backend_form, !socket.assigns.show_add_backend_form)}
  end

  def handle_event("toggle-execution-history", _params, socket) do
    {:noreply, assign(socket, :show_execution_history, !socket.assigns.show_execution_history)}
  end

  def handle_event("refresh-execution-history", _params, socket) do
    send(self(), :refresh_execution_history)
    {:noreply, socket}
  end

  def handle_event("view-results", %{"job_id" => job_id_str}, socket) do
    job_id = String.to_integer(job_id_str)
    job = Enum.find(socket.assigns.past_jobs_page.entries, &(&1.id == job_id))

    {:noreply,
     socket
     |> assign(:modal_results, get_in(job.meta, ["result", "rows"]))
     |> assign(:modal_job_id, job_id)
     |> assign(:modal_node, List.first(job.attempted_by || []))}
  end

  def handle_event("navigate-page", page_str, %{assigns: %{alert: alert, team: team}} = socket)
      when is_binary(page_str) do
    page = String.to_integer(page_str)
    params = if team, do: [t: Phoenix.Param.to_param(team), page: page], else: [page: page]
    to = ~p"/alerts/#{alert.id}" <> "?" <> URI.encode_query(params)

    {:noreply, push_patch(socket, to: to)}
  end

  def handle_event("close-results-modal", _params, socket) do
    {:noreply,
     socket
     |> assign(:modal_results, nil)
     |> assign(:modal_job_id, nil)
     |> assign(:modal_node, nil)}
  end

  def handle_info({:query_string_updated, query_string}, socket) do
    {:noreply, assign(socket, :query_string, query_string)}
  end

  def handle_info({:poll_job, job_id, attempt}, %{assigns: %{alert: alert}} = socket)
      when attempt < @poll_max_attempts do
    job = Repo.get(Oban.Job, job_id)

    if job && job.state in ["completed", "discarded", "cancelled"] do
      current_page = current_page_number(socket)

      {:noreply,
       socket
       |> assign(:triggering_alert, false)
       |> assign(:pending_job_id, nil)
       |> assign(:future_jobs, Alerting.list_future_jobs(alert.id))
       |> assign(:past_jobs_page, paginate_past_jobs(alert.id, current_page))}
    else
      Process.send_after(self(), {:poll_job, job_id, attempt + 1}, @poll_interval)
      {:noreply, socket}
    end
  end

  def handle_info({:poll_job, _job_id, _attempt}, socket) do
    {:noreply,
     socket
     |> assign(:triggering_alert, false)
     |> assign(:pending_job_id, nil)
     |> put_flash(:warning, "Alert job is taking longer than expected. Check back later.")}
  end

  def handle_info(:refresh_execution_history, %{assigns: %{alert: alert}} = socket)
      when not is_nil(alert) do
    current_page = current_page_number(socket)

    socket =
      socket
      |> assign(:future_jobs, Alerting.list_future_jobs(alert.id))
      |> assign(:past_jobs_page, paginate_past_jobs(alert.id, current_page))

    {:noreply, socket}
  end

  def handle_info(:refresh_execution_history, socket) do
    {:noreply, socket}
  end

  defp refresh(%{assigns: assigns} = socket) do
    alerts = Alerting.list_alert_queries(assigns.user)

    assign(socket, :alerts, alerts)
  end

  defp assign_endpoints_and_sources(socket) do
    %{user_id: user_id} = socket.assigns

    socket
    |> assign(
      sources: Logflare.Sources.list_sources_by_user(user_id),
      endpoints: Endpoints.list_endpoints_by(user_id: user_id)
    )
  end

  defp job_state_badge_class(state) do
    case to_string(state) do
      "completed" -> "success"
      "executing" -> "info"
      "scheduled" -> "secondary"
      "available" -> "primary"
      "discarded" -> "danger"
      "cancelled" -> "warning"
      _ -> "secondary"
    end
  end

  defp time_ago(datetime) do
    diff = DateTime.diff(DateTime.utc_now(), datetime, :second)

    cond do
      diff < 15 -> "Just now"
      diff < 60 -> "#{diff}s ago"
      diff < 3_600 -> "#{div(diff, 60)}m ago"
      diff < 86_400 -> "#{div(diff, 3600)}h ago"
      diff < 604_800 -> "#{div(diff, 86400)}d ago"
      true -> Calendar.strftime(datetime, "%Y-%m-%d")
    end
  end

  defp format_bytes(nil), do: nil

  defp format_bytes(bytes) when is_integer(bytes) do
    cond do
      bytes >= 1_099_511_627_776 ->
        "#{:erlang.float_to_binary(bytes / 1_099_511_627_776, decimals: 2)} TB"

      bytes >= 1_073_741_824 ->
        "#{:erlang.float_to_binary(bytes / 1_073_741_824, decimals: 2)} GB"

      bytes >= 1_048_576 ->
        "#{:erlang.float_to_binary(bytes / 1_048_576, decimals: 2)} MB"

      bytes >= 1_024 ->
        "#{:erlang.float_to_binary(bytes / 1_024, decimals: 2)} KB"

      true ->
        "#{bytes} B"
    end
  end

  defp format_bytes(_), do: nil

  defp truncate_reason(reason, max_length \\ 80)

  defp truncate_reason(reason, max_length)
       when is_binary(reason) and byte_size(reason) > max_length do
    String.slice(reason, 0, max_length) <> "..."
  end

  defp truncate_reason(reason, _max_length) when is_binary(reason), do: reason
  defp truncate_reason(reason, max_length), do: truncate_reason(inspect(reason), max_length)

  defp current_page_number(socket) do
    if socket.assigns.past_jobs_page,
      do: socket.assigns.past_jobs_page.page_number,
      else: 1
  end

  defp upsert_alert(alert, user, params) do
    case alert do
      nil -> Alerting.create_alert_query(user, params)
      %_{} -> Alerting.update_alert_query(alert, params)
    end
  end

  defp paginate_past_jobs(alert_query_id, page_num) do
    query = Alerting.past_jobs_query(alert_query_id)
    total = Repo.aggregate(query, :count)
    total_pages = max(ceil(total / @past_jobs_page_size), 1)
    offset_val = (page_num - 1) * @past_jobs_page_size

    entries =
      query
      |> offset(^offset_val)
      |> limit(^@past_jobs_page_size)
      |> Repo.all()

    %{
      entries: entries,
      page_number: page_num,
      total_pages: total_pages,
      total_entries: total
    }
  end
end
