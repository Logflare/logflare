defmodule LogflareWeb.Source.SearchLV do
  @moduledoc """
  Handles all user interactions with the source logs search
  """
  use Phoenix.LiveView
  alias LogflareWeb.Router.Helpers, as: Routes

  alias LogflareWeb.SearchView

  alias Logflare.Logs.SearchQueryExecutor
  alias Logflare.SavedSearches
  alias __MODULE__.SearchOpts
  import Logflare.Logs.Search.Utils
  import LogflareWeb.SearchLV.Utils
  use LogflareWeb.LiveViewUtils
  use LogflareWeb.ModalHelpersLV
  require Logger
  alias Logflare.{Sources, Users}
  @tail_search_interval 500
  @user_idle_interval 300_000
  @default_tailing? true
  @default_chart_aggregate :count
  @default_chart_period :minute

  def render(assigns) do
    SearchView.render("logs_search.html", assigns)
  end

  def mount(%{"source_id" => source_id}, %{"user_id" => user_id}, socket) do
    source =
      source_id
      |> String.to_integer()
      |> Sources.Cache.get_by_id_and_preload()

    Logger.info(
      "#{pid_source_to_string(self(), source)} received params after mount... Connected: #{
        connected?(socket)
      }"
    )

    user = Users.Cache.get_by_and_preload(id: user_id)

    Logger.info("#{pid_to_string(self())} is being mounted... Connected: #{connected?(socket)}")

    socket =
      assign(
        socket,
        source: source,
        log_events: [],
        log_aggregates: [],
        loading: false,
        tailing?: @default_tailing?,
        tailing_paused?: nil,
        tailing_timer: nil,
        user: user,
        flash: %{},
        querystring: "",
        search_op: nil,
        search_op_error: nil,
        search_op_log_events: nil,
        search_op_log_aggregates: nil,
        chart_period: @default_chart_period,
        chart_aggregate: @default_chart_aggregate,
        tailing_timer: nil,
        user_idle_interval: @user_idle_interval,
        active_modal: nil,
        search_tip: gen_search_tip(),
        user_local_timezone: nil,
        use_local_time: true,
        chart_aggregate_enabled?: false,
        last_query_completed_at: nil
      )

    {:ok, socket}
  end

  def handle_params(_params, _uri, socket) do
    {:noreply, socket}
  end

  def handle_event("pause_live_search" = ev, _, %{assigns: prev_assigns} = socket) do
    %{source: %{token: stoken} = source} = prev_assigns
    log_lv_received_event(ev, source)

    socket =
      if prev_assigns.tailing? and !prev_assigns.tailing_paused? do
        maybe_cancel_tailing_timer(socket)
        SearchQueryExecutor.maybe_cancel_query(stoken)

        socket
        |> assign(:tailing?, false)
        |> assign(:tailing_paused?, true)
      else
        socket
      end

    {:noreply, socket}
  end

  def handle_event("resume_live_search" = ev, _, %{assigns: prev_assigns} = socket) do
    %{source: %{token: stoken} = source} = prev_assigns
    log_lv_received_event(ev, source)

    socket =
      if prev_assigns.tailing_paused? do
        socket =
          socket
          |> assign(:tailing_paused?, nil)
          |> assign(:tailing?, true)

        SearchQueryExecutor.maybe_execute_query(stoken, socket.assigns)

        socket
      else
        socket
      end

    {:noreply, socket}
  end

  def handle_event("form_update" = ev, %{"search" => search}, %{assigns: assigns} = socket) do
    source = assigns.source
    log_lv_received_event(ev, source)

    {:ok, search_opts} = SearchOpts.new(assigns, search)

    chart_aggregate_enabled? = search_opts.querystring =~ ~r/chart:\w+/

    warning =
      if search_opts.tailing? and search_opts.querystring =~ "timestamp" do
        "Timestamp field is ignored if live tail search is active"
      else
        nil
      end

    %{chart_aggregate: prev_chart_aggregate, chart_period: prev_chart_period} = assigns

    socket =
      socket
      |> assign(:chart_aggregate_enabled?, chart_aggregate_enabled?)
      |> assign(:querystring, search_opts.querystring)
      |> assign(:tailing?, search_opts.tailing?)
      |> assign_flash(:warning, warning)

    socket =
      if {search_opts.chart_aggregate, search_opts.chart_period} !=
           {prev_chart_aggregate, prev_chart_period} do
        params = %{
          chart_aggregate: "#{search_opts.chart_aggregate}",
          chart_period: "#{search_opts.chart_period}",
          querystring: search_opts.querystring,
          tailing?: search_opts.tailing?
        }

        socket =
          socket
          |> assign(:chart_aggregate, search_opts.chart_aggregate)
          |> assign(:chart_period, search_opts.chart_period)
          |> assign(:log_aggregates, [])
          |> assign(:loading, true)

        :ok = SearchQueryExecutor.maybe_execute_query(source.token, socket.assigns)

        live_redirect(socket,
          to: Routes.live_path(socket, __MODULE__, socket.assigns.source.id, params),
          replace: true
        )
      else
        socket
      end

    {:noreply, socket}
  end

  def handle_event("start_search" = ev, metadata, %{assigns: assigns} = socket) do
    %{id: sid, token: stoken} = assigns.source
    log_lv_received_event(ev, assigns.source)

    params =
      socket.assigns
      |> Map.take([:chart_aggregate, :chart_period, :querystring, :tailing?])

    maybe_cancel_tailing_timer(socket)
    user_local_tz = metadata["search"]["user_local_timezone"]

    socket =
      socket
      |> assign(:log_events, [])
      |> assign(:loading, true)
      |> assign(:tailing_initial?, true)
      |> assign(:user_local_timezone, user_local_tz)
      |> assign_flash(:warning, nil)
      |> assign_flash(:error, nil)
      |> live_redirect(
        to: Routes.live_path(socket, __MODULE__, sid, params),
        replace: true
      )

    SearchQueryExecutor.maybe_execute_query(stoken, assigns)

    {:noreply, socket}
  end

  def handle_event("set_local_time" = ev, metadata, socket) do
    log_lv_received_event(ev, socket.assigns.source)

    use_local_time =
      metadata
      |> Map.get("use_local_time")
      |> String.to_existing_atom()
      |> Kernel.not()

    socket = assign(socket, :use_local_time, use_local_time)

    socket =
      if use_local_time do
        assign(socket, :user_local_timezone, metadata["user_local_timezone"])
      else
        assign(socket, :user_local_timezone, "Etc/UTC")
      end

    {:noreply, socket}
  end

  def handle_event("user_idle" = ev, _, socket) do
    %{token: stoken} = source = socket.assigns.source
    log_lv_received_event(ev, source)

    maybe_cancel_tailing_timer(socket)
    SearchQueryExecutor.maybe_cancel_query(stoken)

    socket = assign_flash(socket, :warning, "Live search paused due to user inactivity.")

    {:noreply, socket}
  end

  def handle_event("save_search" = ev, _, socket) do
    %{source: source, querystring: qs} = socket.assigns
    log_lv_received_event(ev, source)

    case SavedSearches.insert(qs, source) do
      {:ok, _saved_search} ->
        socket = assign_flash(socket, :warning, "Search saved!")
        {:noreply, socket}

      {:error, changeset} ->
        {message, _} = changeset.errors[:querystring]
        socket = assign_flash(socket, :warning, "Save search error: #{message}")
        {:noreply, socket}
    end
  end

  def handle_info({:search_result, search_result}, socket) do
    log_lv_received_event("search_result", socket.assigns.source)

    tailing_timer =
      if socket.assigns.tailing? do
        log_lv(socket.assigns.source, "is scheduling tail search")
        Process.send_after(self(), :schedule_tail_search, @tail_search_interval)
      else
        nil
      end

    warning = warning_message(socket.assigns, search_result)

    log_events = search_result.events.rows

    log_aggregates =
      search_result.aggregates.rows
      |> Enum.reverse()
      |> Enum.map(fn la ->
        Map.update!(
          la,
          "timestamp",
          &SearchView.format_timestamp(&1, socket.assigns.user_local_timezone)
        )
      end)

    socket =
      socket
      |> assign(:log_events, log_events)
      |> assign(:log_aggregates, log_aggregates)
      |> assign(:search_result, search_result.events)
      |> assign(:search_op_error, nil)
      |> assign(:search_op_log_events, search_result.events)
      |> assign(:search_op_log_aggregates, search_result.aggregates)
      |> assign(:tailing_timer, tailing_timer)
      |> assign(:loading, false)
      |> assign(:tailing_initial?, false)
      |> assign(:last_query_completed_at, Timex.now())
      |> assign_flash(:warning, warning)

    {:noreply, socket}
  end

  def handle_info({:search_error = msg, search_op}, %{assigns: %{source: source}} = socket) do
    log_lv_received_info(msg, source)

    socket =
      socket
      |> assign(:search_op_error, search_op)
      |> assign(:search_op_log_events, nil)
      |> assign(:search_op_log_aggregates, nil)
      |> assign_flash(:error, format_error(search_op.error))
      |> assign(:tailing?, false)
      |> assign(:loading, false)

    {:noreply, socket}
  end

  def handle_info(:schedule_tail_search = msg, %{assigns: assigns} = socket) do
    if socket.assigns.tailing? do
      log_lv_received_info(msg, assigns.source)
      SearchQueryExecutor.maybe_execute_query(assigns.source.token, assigns)
    end

    {:noreply, socket}
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
