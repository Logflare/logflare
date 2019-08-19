defmodule LogflareWeb.Source.SearchLV do
  @moduledoc """
  Handles all user interactions with the source logs search
  """
  use Phoenix.LiveView
  alias LogflareWeb.SourceView

  alias Logflare.Logs.Search
  alias Logflare.Logs.Search.SearchOperation, as: SO
  alias Logflare.LogEvent
  alias Logflare.Logs.SearchQueryExecutor
  alias __MODULE__.SearchParams
  import Logflare.Logs.Search.Utils
  require Logger
  @tail_search_interval 5_000
  @user_idle_interval 120_000

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
      assign(socket,
        querystring: qs || "",
        log_events: [],
        tailing?: is_nil(session[:tailing?]) || session[:tailing?],
        source: source,
        loading: false,
        user: user,
        flash: %{},
        search_op: nil,
        tailing_timer: nil,
        user_idle_interval: @user_idle_interval,
        active_modal: nil,
        first_search?: false,
        search_tip: gen_search_tip()
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

  def handle_event("start_search" = ev, _, socket) do
    log_lv_received_event(ev, socket.assigns.source)
    if socket.assigns.tailing_timer, do: Process.cancel_timer(socket.assigns.tailing_timer)

    socket =
      socket
      |> assign(:log_events, [])
      |> assign(:loading, true)
      |> assign(:tailing_initial?, true)
      |> assign_flash(:warning, nil)
      |> assign_flash(:error, nil)

    maybe_execute_query(socket.assigns)

    {:noreply, socket}
  end

  def handle_event("activate_modal" = ev, modal_id, socket) do
    log_lv_received_event(ev, socket.assigns.source)
    {:noreply, assign(socket, :active_modal, modal_id)}
  end

  def handle_event("deactivate_modal" = ev, _, socket) do
    log_lv_received_event(ev, socket.assigns.source)
    {:noreply, assign(socket, :active_modal, nil)}
  end

  def handle_event("remove_flash" = ev, key, socket) do
    log_lv_received_event(ev, socket.assigns.source)
    key = String.to_existing_atom(key)
    socket = assign_flash(socket, key, nil)
    {:noreply, socket}
  end

  def handle_event("user_idle" = ev, _, socket) do
    log_lv_received_event(ev, socket.assigns.source)
    socket = assign_flash(socket, :warning, "Live search paused due to user inactivity.")

    {:noreply, socket}
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

    tailing? = socket.assigns.tailing?
    querystring = socket.assigns.querystring
    log_events_empty? = search_op.rows == []

    warning =
      cond do
        log_events_empty? and not tailing? ->
          "No logs matching your search query"

        log_events_empty? and tailing? ->
          "No logs matching your search query ingested during last 24 hours..."

        querystring == "" and log_events_empty? and tailing? ->
          "No logs ingested during last 24 hours..."

        true ->
          nil
      end

    socket =
      socket
      |> assign(:log_events, search_op.rows)
      |> assign(:search_op, search_op)
      |> assign(:tailing_timer, tailing_timer)
      |> assign(:loading, false)
      |> assign(:tailing_initial?, false)
      |> assign(:first_search?, socket.assigns.tailing_initial?)
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

  def assign_flash(socket, key, message) do
    flash = socket.assigns.flash
    assign(socket, flash: put_in(flash, [key], message))
  end

  def maybe_execute_query(assigns) do
    assigns.source.token
    |> SearchQueryExecutor.name()
    |> Process.whereis()
    |> if do
      :ok = SearchQueryExecutor.query(assigns)
    else
      Logger.error("Search Query Executor process for not alive")
    end
  end
end
