defmodule LogflareWeb.Source.SearchLV do
  @moduledoc """
  Handles all user interactions with the source logs search
  """
  use Phoenix.LiveView, layout: {LogflareWeb.LayoutView, "live.html"}
  alias LogflareWeb.Router.Helpers, as: Routes

  alias LogflareWeb.SearchView

  alias Logflare.Logs.SearchQueryExecutor
  alias Logflare.SavedSearches
  alias __MODULE__.SearchOpts
  alias Logflare.Lql
  alias Logflare.Lql.{ChartRule, FilterRule}
  import Logflare.Logs.Search.Utils
  import LogflareWeb.SearchLV.Utils
  use LogflareWeb.LiveViewUtils
  use LogflareWeb.ModalsLVHelpers
  require Logger
  alias Logflare.{Sources, Users}
  @tail_search_interval 500
  @user_idle_interval 300_000

  def render(assigns) do
    assigns = %{
      assigns
      | chart_aggregate_enabled?: search_agg_controls_enabled?(assigns.lql_rules)
    }

    SearchView.render("logs_search.html", assigns)
  end

  def mount(%{"source_id" => source_id} = params, %{"user_id" => user_id}, socket) do
    Logger.info("#{pid_to_string(self())} is being mounted... Connected: #{connected?(socket)}")

    source =
      source_id
      |> String.to_integer()
      |> Sources.Cache.get_by_id_and_preload()
      |> Sources.put_bq_table_data()

    user = Users.Cache.get_by_and_preload(id: user_id)

    {:ok, search_opts} = SearchOpts.new(params)
    {:ok, lql_rules} = Lql.decode(search_opts.querystring, source.bq_table_schema)

    lql_rules =
      if Enum.find(lql_rules, &match?(%ChartRule{}, &1)) do
        lql_rules
      else
        [
          %ChartRule{
            aggregate: :count,
            path: "timestamp",
            period: :minute,
            value_type: nil
          }
          | lql_rules
        ]
      end

    qs = Lql.encode!(lql_rules)

    socket =
      assign(
        socket,
        source: source,
        log_events: [],
        log_aggregates: [],
        loading: false,
        tailing?: search_opts.tailing?,
        tailing_paused?: nil,
        tailing_timer: nil,
        user: user,
        notifications: %{},
        querystring: qs,
        search_op: nil,
        search_op_error: nil,
        search_op_log_events: nil,
        search_op_log_aggregates: nil,
        chart_period: search_opts.chart_period,
        chart_aggregate: search_opts.chart_aggregate,
        chart_aggregate_enabled?: nil,
        tailing_timer: nil,
        user_idle_interval: @user_idle_interval,
        active_modal: nil,
        search_tip: gen_search_tip(),
        user_local_timezone: nil,
        use_local_time: true,
        last_query_completed_at: nil,
        lql_rules: lql_rules
      )

    {:ok, socket}
  end

  def handle_params(_params, _uri, socket) do
    {:noreply, socket}
  end

  def handle_event("stop_live_search" = ev, _, %{assigns: prev_assigns} = socket) do
    %{source: %{token: stoken} = source} = prev_assigns
    log_lv_received_event(ev, source)

    socket =
      if prev_assigns.tailing? do
        maybe_cancel_tailing_timer(socket)
        SearchQueryExecutor.maybe_cancel_query(stoken)

        assign(socket, :tailing?, false)
      else
        socket
      end

    {:noreply, socket}
  end

  def handle_event("start_live_search" = ev, _, %{assigns: prev_assigns} = socket) do
    %{source: %{token: stoken} = source} = prev_assigns
    log_lv_received_event(ev, source)

    socket =
      if not prev_assigns.tailing? do
        socket = assign(socket, :tailing?, true)

        SearchQueryExecutor.maybe_execute_query(stoken, socket.assigns)

        socket
      else
        socket
      end

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

  def handle_event("form_update" = ev, %{"search" => search}, %{assigns: prev_assigns} = socket) do
    source = prev_assigns.source
    log_lv_received_event(ev, source)

    {:ok, search_opts} = SearchOpts.new(prev_assigns, search)

    socket =
      socket
      |> assign(:querystring, search_opts.querystring)
      |> assign(:tailing?, search_opts.tailing?)

    %{chart_aggregate: prev_agg_fn, chart_period: prev_agg_period} = prev_assigns

    socket =
      if search_opts.chart_aggregate != prev_agg_fn or
           search_opts.chart_period != prev_agg_period do
        params = %{
          chart_aggregate: "#{search_opts.chart_aggregate}",
          chart_period: "#{search_opts.chart_period}",
          querystring: search_opts.querystring,
          tailing?: search_opts.tailing?
        }

        {:ok, lql_rules} = Lql.decode(search_opts.querystring, source.bq_table_schema)

        qs =
          lql_rules
          |> Enum.map(fn
            %ChartRule{} = lqlc ->
              %{lqlc | aggregate: search_opts.chart_aggregate, period: search_opts.chart_period}

            x ->
              x
          end)
          |> Lql.encode!()

        socket =
          socket
          |> assign(:chart_aggregate, search_opts.chart_aggregate)
          |> assign(:chart_period, search_opts.chart_period)
          |> assign(:querystring, qs)
          |> assign(:lql_rules, lql_rules)
          |> assign(:log_aggregates, [])
          |> assign(:loading, true)

        :ok = SearchQueryExecutor.maybe_execute_query(source.token, socket.assigns)

        push_patch(socket,
          to: Routes.live_path(socket, __MODULE__, socket.assigns.source.id, params),
          replace: true
        )
      else
        socket
      end

    {:noreply, socket}
  end

  def handle_event(
        "datepicker_update" = ev,
        %{"querystring" => ts_qs},
        %{assigns: assigns} = socket
      ) do
    log_lv_received_event(ev, socket.assigns.source)
    {:ok, ts_rules} = Lql.decode(ts_qs, assigns.source.bq_table_schema)

    lql_rules =
      assigns.lql_rules
      |> Enum.reject(&(&1.path == "timestamp" and match?(%FilterRule{}, &1)))
      |> Enum.concat(ts_rules)

    qs = Lql.encode!(lql_rules)

    {:noreply,
     socket
     |> assign(:lql_rules, lql_rules)
     |> assign(:querystring, qs)}
  end

  def handle_event(
        "start_search" = ev,
        %{"search" => %{"querystring" => querystring}},
        %{assigns: prev_assigns} = socket
      ) do
    %{id: sid, token: stoken} = prev_assigns.source
    log_lv_received_event(ev, prev_assigns.source)

    params =
      prev_assigns
      |> Map.take([:chart_aggregate, :chart_period, :querystring, :tailing?])

    {:ok, lql_rules} = Lql.decode(querystring, prev_assigns.source.bq_table_schema)
    qs = Lql.encode!(lql_rules)

    maybe_cancel_tailing_timer(socket)

    socket =
      socket
      |> assign(:lql_rules, lql_rules)
      |> assign(:querystring, qs)
      |> assign(:log_events, [])
      |> assign(:loading, true)
      |> assign(:tailing_initial?, true)
      |> assign_notifications(:warning, nil)
      |> assign_notifications(:error, nil)
      |> push_patch(
        to: Routes.live_path(socket, __MODULE__, sid, params),
        replace: true
      )

    SearchQueryExecutor.maybe_execute_query(stoken, socket.assigns)

    {:noreply, socket}
  end

  def handle_event("set_user_local_timezone", metadata, socket) do
    socket = assign(socket, :user_local_timezone, metadata["tz"])
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

    {:noreply, socket}
  end

  def handle_event("user_idle" = ev, _, socket) do
    %{token: stoken} = source = socket.assigns.source
    log_lv_received_event(ev, source)

    maybe_cancel_tailing_timer(socket)
    SearchQueryExecutor.maybe_cancel_query(stoken)

    socket = assign_notifications(socket, :warning, "Live search paused due to user inactivity.")

    {:noreply, socket}
  end

  def handle_event("save_search" = ev, _, socket) do
    %{source: source, querystring: qs, lql_rules: lql_rules} = socket.assigns
    log_lv_received_event(ev, source)

    case SavedSearches.save_by_user(qs, lql_rules, source) do
      {:ok, _saved_search} ->
        socket = assign_notifications(socket, :warning, "Search saved!")
        {:noreply, socket}

      {:error, changeset} ->
        {message, _} = changeset.errors[:querystring]
        socket = assign_notifications(socket, :warning, "Save search error: #{message}")
        {:noreply, socket}
    end
  end

  def handle_event("reset_search", _, %{assigns: assigns} = socket) do
    {:ok, sopts} = SearchOpts.new(%{"querystring" => ""})
    lql_rules = Lql.decode!(sopts.querystring, assigns.source.bq_table_schema)
    qs = Lql.encode!(lql_rules)

    socket =
      socket
      |> assign(:querystring, qs)
      |> assign(:lql_rules, lql_rules)

    {:noreply, socket}
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

    warning =
      cond do
        match?({:warning, _}, search_result.aggregates.status) ->
          {:warning, message} = search_result.aggregates.status
          message

        match?({:warning, _}, search_result.events.status) ->
          {:warning, message} = search_result.events.status
          message

        true ->
          warning_message(socket.assigns, search_result)
      end

    log_events = search_result.events.rows

    timezone =
      if socket.assigns.use_local_time do
        socket.assigns.user_local_timezone
      else
        "Etc/UTC"
      end

    log_aggregates =
      search_result.aggregates.rows
      |> Enum.reverse()
      |> Enum.map(fn la ->
        Map.update!(
          la,
          :timestamp,
          &LogflareWeb.Helpers.BqSchema.format_timestamp(&1, timezone)
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
      |> assign_notifications(:warning, warning)

    {:noreply, socket}
  end

  def handle_info({:search_error = msg, search_op}, %{assigns: %{source: source}} = socket) do
    log_lv_received_info(msg, source)

    error_notificaton =
      case search_op.error do
        :halted ->
          {:halted, halted_message} = search_op.status
          "Search halted: " <> halted_message

        err ->
          format_error(err)
      end

    socket =
      socket
      |> assign(:search_op_error, search_op)
      |> assign(:search_op_log_events, nil)
      |> assign(:search_op_log_aggregates, nil)
      |> assign_notifications(:error, error_notificaton)
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

  defp search_agg_controls_enabled?(lql_rules) do
    lql_rules
    |> Enum.find(%{}, &match?(%ChartRule{}, &1))
    |> Map.get(:value_type)
    |> Kernel.in([:integer, :float])
  end
end
