defmodule LogflareWeb.FetchQueriesLive do
  @moduledoc """
  LiveView for managing fetch queries.

  This module provides CRUD operations for fetch queries with team-scoped access.
  Following the Endpoints and Alerts patterns.
  """

  use LogflareWeb, :live_view

  alias Logflare.Backends
  alias Logflare.FetchQueries
  alias Logflare.FetchQueries.FetchQuery
  alias Logflare.Sources

  embed_templates "fetch_queries/actions/*"
  embed_templates "fetch_queries/components/*"

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    user = socket.assigns.user

    unless LogflareWeb.Utils.flag("fetch_jobs", user) do
      raise LogflareWeb.ErrorsLive.InvalidResourceError
    end

    backends = Backends.list_backends_by_user_access(user)
    sources = Sources.list_sources_by_user(user)

    {:ok,
     assign(socket,
       fetch_queries: [],
       backends: backends,
       sources: sources,
       errors_visible: %{}
     )}
  end

  @impl Phoenix.LiveView
  def handle_params(params, _uri, socket) do
    case socket.assigns.live_action do
      :index -> {:noreply, load_index(socket)}
      :new -> {:noreply, prepare_new(socket)}
      :show -> {:noreply, show(socket, params)}
      :edit -> {:noreply, edit(socket, params)}
    end
  end

  @impl Phoenix.LiveView
  def handle_event("save", %{"fetch_query" => params}, socket) do
    user = socket.assigns.user

    case socket.assigns.live_action do
      :new ->
        create_fetch_query(socket, user, params)

      :edit ->
        update_fetch_query(socket, user, params)
    end
  end

  def handle_event("delete", %{"id" => id}, socket) do
    user = socket.assigns.user

    case FetchQueries.get_fetch_query_by_user_access(user, id) do
      nil ->
        {:noreply, put_flash(socket, :error, "Fetch query not found")}

      fetch_query ->
        case FetchQueries.delete_fetch_query(fetch_query) do
          {:ok, _} ->
            {:noreply,
             socket
             |> put_flash(:info, "Fetch query deleted successfully")
             |> push_patch(to: ~p"/fetch")}

          {:error, _} ->
            {:noreply, put_flash(socket, :error, "Failed to delete fetch query")}
        end
    end
  end

  def handle_event("validate", %{"fetch_query" => params}, socket) do
    changeset =
      FetchQuery.changeset(
        %FetchQuery{},
        Map.put(params, "user_id", socket.assigns.user.id)
      )

    {:noreply, assign(socket, form: to_form(changeset))}
  end

  def handle_event("trigger-now", _params, socket) do
    fetch_query = socket.assigns.fetch_query

    case FetchQueries.trigger_fetch_query_now(fetch_query.id) do
      {:ok, _job} ->
        jobs = FetchQueries.list_execution_history(fetch_query.id)
        {future_jobs, past_jobs} = FetchQueries.partition_jobs_by_time(jobs)

        # Schedule a refresh 2 seconds after triggering
        if connected?(socket) do
          Process.send_after(self(), :refresh_execution_history, 2000)
        end

        {:noreply,
         socket
         |> assign(future_jobs: future_jobs, past_jobs: past_jobs)
         |> put_flash(:info, "Fetch query triggered successfully. Job will run immediately.")}

      {:error, reason} ->
        {:noreply,
         socket
         |> put_flash(:error, "Failed to trigger fetch query: #{inspect(reason)}")}
    end
  end

  def handle_event("toggle-error", %{"error-id" => error_id}, socket) do
    {:noreply, toggle_error(socket, error_id)}
  end

  @impl Phoenix.LiveView
  def handle_info(:refresh_execution_history, socket) do
    # Refresh the execution history for the current fetch query
    if socket.assigns[:fetch_query] do
      fetch_query = socket.assigns.fetch_query
      jobs = FetchQueries.list_execution_history(fetch_query.id)
      {future_jobs, past_jobs} = FetchQueries.partition_jobs_by_time(jobs)

      {:noreply, assign(socket, future_jobs: future_jobs, past_jobs: past_jobs)}
    else
      {:noreply, socket}
    end
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    case assigns.live_action do
      :index -> ~H"<.index {assigns} />"
      :new -> ~H"<.new {assigns} />"
      :show -> ~H"<.show {assigns} />"
      :edit -> ~H"<.edit {assigns} />"
    end
  end

  defp create_fetch_query(socket, user, params) do
    params = Map.put(params, "user_id", user.id)

    case FetchQueries.create_fetch_query(params) do
      {:ok, fetch_query} ->
        {:noreply,
         socket
         |> put_flash(:info, "Fetch query created successfully")
         |> push_patch(to: ~p"/fetch/#{fetch_query.id}")}

      {:error, changeset} ->
        {:noreply, assign(socket, form: to_form(changeset))}
    end
  end

  defp update_fetch_query(socket, _user, params) do
    fetch_query = socket.assigns.fetch_query

    case FetchQueries.update_fetch_query(fetch_query, params) do
      {:ok, _} ->
        {:noreply,
         socket
         |> put_flash(:info, "Fetch query updated successfully")
         |> push_patch(to: ~p"/fetch/#{fetch_query.id}")}

      {:error, changeset} ->
        {:noreply, assign(socket, form: to_form(changeset))}
    end
  end

  defp load_index(socket) do
    user = socket.assigns.user
    fetch_queries = FetchQueries.list_fetch_queries_by_user_access(user)
    assign(socket, fetch_queries: fetch_queries)
  end

  defp prepare_new(socket) do
    changeset = FetchQuery.changeset(%FetchQuery{}, %{"user_id" => socket.assigns.user.id})
    assign(socket, form: to_form(changeset))
  end

  defp show(socket, %{"id" => id}) do
    user = socket.assigns.user

    case FetchQueries.get_fetch_query_by_user_access(user, id) do
      nil ->
        socket
        |> put_flash(:error, "Fetch query not found")
        |> push_navigate(to: ~p"/fetch")

      fetch_query ->
        fetch_query = FetchQueries.preload_fetch_query(fetch_query)
        jobs = FetchQueries.list_execution_history(fetch_query.id)
        {future_jobs, past_jobs} = FetchQueries.partition_jobs_by_time(jobs)

        assign(socket,
          fetch_query: fetch_query,
          future_jobs: future_jobs,
          past_jobs: past_jobs
        )
    end
  end

  defp edit(socket, %{"id" => id}) do
    user = socket.assigns.user

    case FetchQueries.get_fetch_query_by_user_access(user, id) do
      nil ->
        socket
        |> put_flash(:error, "Fetch query not found")
        |> push_navigate(to: ~p"/fetch")

      fetch_query ->
        fetch_query = FetchQueries.preload_fetch_query(fetch_query)
        changeset = FetchQuery.changeset(fetch_query, %{})
        assign(socket, fetch_query: fetch_query, form: to_form(changeset))
    end
  end

  def get_query_placeholder(%Phoenix.HTML.Form{source: changeset}) do
    get_query_placeholder(changeset)
  end

  def get_query_placeholder(changeset) do
    case Ecto.Changeset.get_field(changeset, :language) do
      "jsonpath" -> "$.data[*] - JSONPath to extract fields"
      "lql" -> "SELECT * FROM ... - LQL query"
      "pg_sql" -> "SELECT * FROM ... - PostgreSQL query"
      "bq_sql" -> "SELECT * FROM ... - BigQuery SQL query"
      _ -> "SELECT * FROM ... - Enter your query"
    end
  end

  def format_job_state(state) do
    case state do
      "available" -> "Queued"
      "scheduled" -> "Scheduled"
      "executing" -> "Running"
      "completed" -> "Completed"
      "discarded" -> "Failed"
      "cancelled" -> "Cancelled"
      _ -> String.capitalize(state)
    end
  end

  def job_state_badge_class(state) do
    case state do
      "available" -> "primary"
      "scheduled" -> "primary"
      "executing" -> "warning"
      "completed" -> "success"
      "discarded" -> "danger"
      "cancelled" -> "secondary"
      _ -> "secondary"
    end
  end

  def format_datetime(nil), do: "—"

  def format_datetime(datetime) when is_binary(datetime) do
    case DateTime.from_iso8601(datetime) do
      {:ok, dt, _} -> format_datetime(dt)
      :error -> "—"
    end
  end

  def format_datetime(datetime) do
    datetime
    |> DateTime.shift_zone!("Etc/UTC")
    |> Calendar.strftime("%Y-%m-%d %H:%M:%S UTC")
  end

  def calculate_duration(nil, nil), do: "—"
  def calculate_duration(nil, _), do: "—"
  def calculate_duration(_, nil), do: "—"

  def calculate_duration(started, completed) when is_binary(started) and is_binary(completed) do
    with {:ok, start_dt, _} <- DateTime.from_iso8601(started),
         {:ok, end_dt, _} <- DateTime.from_iso8601(completed) do
      calculate_duration(start_dt, end_dt)
    else
      _ -> "—"
    end
  end

  def calculate_duration(started, completed) do
    diff = DateTime.diff(completed, started, :millisecond)

    cond do
      diff < 1000 -> "#{diff}ms"
      diff < 60000 -> "#{div(diff, 1000)}s"
      true -> "#{div(diff, 60000)}m #{rem(div(diff, 1000), 60)}s"
    end
  end

  def format_job_errors([]), do: "—"
  def format_job_errors(nil), do: "—"

  def format_job_errors(errors) when is_list(errors) do
    errors
    |> Enum.map(fn
      {_stage, message} when is_binary(message) -> message
      message when is_binary(message) -> message
      msg -> inspect(msg)
    end)
    |> Enum.join("; ")
  end

  def error_summary(full_error) when is_binary(full_error) and full_error != "—" do
    # Extract just the first line of the error
    full_error
    |> String.split("\n")
    |> List.first()
    |> String.slice(0..99)
  end

  def error_summary(_), do: "—"

  def has_error?(full_error) when is_binary(full_error) and full_error != "—", do: true
  def has_error?(_), do: false

  def toggle_error(socket, error_id) do
    errors_visible = socket.assigns[:errors_visible] || %{}
    updated_errors = Map.update(errors_visible, error_id, true, &(!&1))
    assign(socket, errors_visible: updated_errors)
  end
end
