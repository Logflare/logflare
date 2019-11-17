defmodule LogflareWeb.Source.SearchLV do
  @moduledoc """
  Handles all user interactions with the source logs search
  """
  use Phoenix.LiveView
  alias LogflareWeb.SourceView

  alias Logflare.Logs.SearchQueryExecutor
  alias Logflare.SavedSearches
  alias __MODULE__.SearchParams
  import Logflare.Logs.Search.Utils
  require Logger
  @tail_search_interval 1_000
  @user_idle_interval 3_000_000

  def render(assigns) do
    Phoenix.View.render(SourceView, "logs_search.html", assigns)
  end

  def mount(session, socket) do
    %{source: source, user: user, querystring: qs} = session

    Logger.info(
      "#{pid_source_to_string(self(), source)} is being mounted... Connected: #{
        connected?(socket)
      }"
    )

    socket =
      assign(
        socket,
        querystring: qs || "",
        log_events: [],
        log_aggregates: [],
        tailing?: is_nil(session[:tailing?]) || session[:tailing?],
        source: source,
        loading: false,
        user: user,
        flash: %{},
        search_op: nil,
        tailing_timer: nil,
        user_idle_interval: @user_idle_interval,
        active_modal: nil,
        search_tip: gen_search_tip(),
        user_local_timezone: nil,
        use_local_time: true,
        search_chart_period: session[:search_chart_period]
      )

    {:ok, socket}
  end

  def handle_event("form_update" = ev, %{"search" => search}, socket) do
    log_lv_received_event(ev, socket.assigns.source)

    params = SearchParams.new(search)

    querystring = params[:querystring] || ""
    tailing? = params[:tailing?] || false

    warning =
      if tailing? && String.contains?(querystring, "timestamp") do
        "Timestamp field is ignored if live tail search is active"
      else
        nil
      end

    socket =
      socket
      |> assign(:tailing?, tailing?)
      |> assign(:querystring, querystring)
      |> assign_flash(:warning, warning)

    {:noreply, socket}
  end

  def handle_event("start_search" = ev, metadata, socket) do
    log_lv_received_event(ev, socket.assigns.source)
    if socket.assigns.tailing_timer, do: Process.cancel_timer(socket.assigns.tailing_timer)
    user_local_tz = metadata["search"]["user_local_timezone"]

    search_chart_period =
      case hd(metadata["search"]["search_chart_period"]) do
        "day" -> :day
        "hour" -> :hour
        "minute" -> :minute
        "second" -> :second
      end

    socket =
      socket
      |> assign(:log_events, [])
      |> assign(:loading, true)
      |> assign(:tailing_initial?, true)
      |> assign(:user_local_timezone, user_local_tz)
      |> assign(:search_chart_period, search_chart_period)
      |> assign_flash(:warning, nil)
      |> assign_flash(:error, nil)

    maybe_execute_query(socket.assigns)

    {:noreply, socket}
  end

  def handle_event("set_local_time" = ev, metadata, socket) do
    log_lv_received_event(ev, socket.assigns.source)

    use_local_time =
      metadata
      |> Map.get("use_local_time")
      |> String.to_existing_atom()
      |> Kernel.not()

    {:noreply, assign(socket, use_local_time: use_local_time)}
  end

  def handle_event("activate_modal" = ev, metadata, socket) do
    log_lv_received_event(ev, socket.assigns.source)
    modal_id = metadata["modal_id"]
    {:noreply, assign(socket, :active_modal, modal_id)}
  end

  def handle_event("deactivate_modal" = ev, _, socket) do
    log_lv_received_event(ev, socket.assigns.source)
    {:noreply, assign(socket, :active_modal, nil)}
  end

  def handle_event("remove_flash" = ev, metadata, socket) do
    log_lv_received_event(ev, socket.assigns.source)
    key = String.to_existing_atom(metadata["flash_key"])
    socket = assign_flash(socket, key, nil)
    {:noreply, socket}
  end

  def handle_event("user_idle" = ev, _, socket) do
    log_lv_received_event(ev, socket.assigns.source)
    socket = assign_flash(socket, :warning, "Live search paused due to user inactivity.")

    {:noreply, socket}
  end

  def handle_event("save_search" = ev, _, socket) do
    log_lv_received_event(ev, socket.assigns.source)

    case SavedSearches.insert(socket.assigns.querystring, socket.assigns.source) do
      {:ok, saved_search} ->
        socket = assign_flash(socket, :warning, "Search saved: #{saved_search.querystring}")
        {:noreply, socket}

      {:error, _changeset} ->
        socket = assign_flash(socket, :warning, "Search not saved!")
        {:noreply, socket}
    end
  end

  def handle_info({:search_result, search_op}, socket) do
    log_lv_received_event("search_result", socket.assigns.source)

    tailing_timer =
      if socket.assigns.tailing? do
        log_lv(socket.assigns.source, "is scheduling tail search")
        Process.send_after(self(), :schedule_tail_search, @tail_search_interval)
      else
        nil
      end

    warning = warning_message(socket.assigns, search_result)

    socket
    |> assign(:log_events, search_result.events)
    |> assign(:log_aggregates, search_result.aggregates)
    |> assign(:search_result, search_result.events)
    |> assign(:tailing_timer, tailing_timer)
    |> assign(:loading, false)
    |> assign(:tailing_initial?, false)
    |> assign_flash(:warning, warning)

    {:noreply, socket}
  end

  def handle_info({:search_error = msg, search_op}, socket) do
    log_lv_received_info(msg, socket.assigns.source)

    socket =
      socket
      |> assign_flash(:error, format_error(search_op.error))
      |> assign(:loading, false)

    {:noreply, socket}
  end

  def handle_info(:schedule_tail_search = msg, socket) do
    if socket.assigns.tailing? do
      log_lv_received_info(msg, socket.assigns.source)
      maybe_execute_query(socket.assigns)
    end

    {:noreply, socket}
  end

  defp assign_flash(socket, key, message) do
    flash = socket.assigns.flash
    assign(socket, flash: put_in(flash, [key], message))
  end

  defp maybe_execute_query(assigns) do
    assigns.source.token
    |> SearchQueryExecutor.name()
    |> Process.whereis()
    |> if do
      :ok = SearchQueryExecutor.query(assigns)
    else
      Logger.error("Search Query Executor process for not alive")
    end
  end

  defp warning_message(assigns, search_op) do
    tailing? = assigns.tailing?
    querystring = assigns.querystring
    log_events_empty? = search_op.events.rows == []

    cond do
      log_events_empty? and not tailing? ->
        "No log events matching your search query."

      log_events_empty? and tailing? ->
        "No log events matching your search query ingested during last 24 hours..."

      querystring == "" and log_events_empty? and tailing? ->
        "No log events ingested during last 24 hours..."

      true ->
        nil
    end
  end
end
