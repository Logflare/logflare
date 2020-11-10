defmodule LogflareWeb.Source.SearchLV do
  @moduledoc """
  Handles all user interactions with the source logs search
  """
  use LogflareWeb, :live_view

  alias Logflare.Logs.SearchQueryExecutor
  alias Logflare.Lql
  alias Logflare.Lql.ChartRule
  alias Logflare.SavedSearches
  alias Logflare.{Sources, Users, Plans}
  alias LogflareWeb.Helpers.BqSchema, as: BqSchemaHelpers
  alias LogflareWeb.Router.Helpers, as: Routes
  alias LogflareWeb.SearchView

  import Logflare.Logs.Search.Utils
  import LogflareWeb.SearchLV.Utils

  use LogflareWeb.LiveViewUtils
  use LogflareWeb.ModalsLVHelpers

  require Logger

  @tail_search_interval 500
  @user_idle_interval :timer.minutes(5)
  @default_qs "c:count(*) c:group_by(t::minute)"
  @default_assigns [
    log_events: [],
    log_aggregates: [],
    loading: false,
    chart_loading?: true,
    tailing?: true,
    tailing_timer: nil,
    notifications: %{},
    search_op: nil,
    search_op_error: nil,
    search_op_log_events: nil,
    search_op_log_aggregates: nil,
    chart_aggregate_enabled?: nil,
    user_idle_interval: @user_idle_interval,
    active_modal: nil,
    user_local_timezone: nil,
    use_local_time: true,
    last_query_completed_at: nil,
    lql_rules: [],
    querystring: ""
  ]

  def mount(params, session, socket) do
    Logger.info("#{pid_to_string(self())} is being mounted... Connected: #{connected?(socket)}")

    socket =
      if connected?(socket) do
        mount_connected(params, session, socket)
      else
        mount_disconnected(params, session, socket)
      end

    {:ok, socket}
  end

  def handle_params(%{"querystring" => qs}, _uri, socket) do
    source = socket.assigns.source

    socket =
      with {:ok, lql_rules} <- Lql.decode(qs, source.bq_table_schema) do
        lql_rules = Lql.Utils.put_new_chart_rule(lql_rules, Lql.Utils.default_chart_rule())
        qs = Lql.encode!(lql_rules)

        stale_log_events =
          Enum.map(socket.assigns.log_events, &Map.put(&1, :is_from_stale_query?, true))

        socket =
          socket
          |> assign(:loading, true)
          |> assign(:chart_loading?, true)
          |> assign(:tailing_initial?, true)
          |> assign(:log_events, stale_log_events)
          |> assign(:log_aggregates, [])
          |> assign(:lql_rules, lql_rules)
          |> assign(:querystring, qs)

        kickoff_queries(source.token, socket.assigns)

        socket
      else
        {:error, error} ->
          error_socket(socket, error)
      end

    {:noreply, socket}
  end

  def handle_params(_params, _uri, socket) do
    {:noreply, socket}
  end

  def render(assigns) do
    SearchView.render("logs_search.html", %{
      assigns
      | chart_aggregate_enabled?: search_agg_controls_enabled?(assigns.lql_rules)
    })
  end

  def mount_disconnected(
        %{"source_id" => source_id} = params,
        %{"user_id" => user_id} = _session,
        socket
      ) do
    source = Sources.Cache.get_source_for_lv_param(source_id)
    user = Users.get_by_and_preload(id: user_id)

    %{querystring: querystring, tailing?: tailing?} = prepare_params(params)

    socket =
      socket
      |> assign(@default_assigns)
      |> assign(
        source: source,
        tailing?: tailing?,
        user: user,
        loading: true,
        chart_loading: true,
        notifications: %{},
        search_tip: gen_search_tip(),
        use_local_time: true,
        querystring: querystring
      )

    socket =
      with {:ok, lql_rules} <- Lql.decode(querystring, source.bq_table_schema) do
        lql_rules = lql_rules |> Lql.Utils.put_new_chart_rule(Lql.Utils.default_chart_rule())
        optimizedqs = Lql.encode!(lql_rules)

        socket
        |> assign(:lql_rules, lql_rules)
        |> assign(:querystring, optimizedqs)
      else
        {:error, error} ->
          maybe_cancel_tailing_timer(socket)

          error_socket(socket, error)
      end

    if user && (user.admin or source.user_id == user.id) do
      socket
    else
      redirect(socket, to: "/")
    end
  end

  def mount_connected(
        %{"source_id" => source_id} = params,
        %{"user_id" => user_id} = _session,
        socket
      ) do
    user_timezone = get_connect_params(socket)["user_timezone"]

    user_timezone =
      if Timex.Timezone.exists?(user_timezone) do
        user_timezone
      else
        "Etc/UTC"
      end

    source = Sources.Cache.get_source_for_lv_param(source_id)

    user = Users.get_by_and_preload(id: user_id)
    %{querystring: querystring, tailing?: tailing?} = prepare_params(params)

    socket =
      socket
      |> assign(@default_assigns)
      |> assign(
        source: source,
        tailing?: tailing?,
        tailing_initial?: true,
        loading: true,
        chart_loading?: true,
        user: user,
        notifications: %{},
        search_tip: gen_search_tip(),
        user_local_timezone: user_timezone,
        use_local_time: true
      )

    with {:ok, lql_rules} <- Lql.decode(querystring, source.bq_table_schema) do
      lql_rules = lql_rules |> Lql.Utils.put_new_chart_rule(Lql.Utils.default_chart_rule())
      optimizedqs = Lql.encode!(lql_rules)

      socket =
        socket
        |> assign(:lql_rules, lql_rules)
        |> assign(:querystring, optimizedqs)

      socket
    else
      {:error, error} ->
        maybe_cancel_tailing_timer(socket)
        SearchQueryExecutor.maybe_cancel_query(source.token)

        error_socket(socket, error)
    end
  end

  defp prepare_params(params) do
    params
    |> case do
      %{"querystring" => ""} = p ->
        %{p | "querystring" => @default_qs}

      %{"q" => q} = p ->
        Map.put(p, "querystring", q)

      p ->
        p
    end
    |> Map.put_new("querystring", @default_qs)
    |> Map.put_new("tailing?", "true")
    |> Map.update!("tailing?", &String.to_existing_atom/1)
    |> MapKeys.to_atoms_unsafe!()
    |> Map.take([:querystring, :tailing?])
  end

  def handle_event("stop_live_search" = ev, _, %{assigns: prev_assigns} = socket) do
    %{source: %{token: stoken} = source} = prev_assigns
    log_lv_received_event(ev, source)

    socket =
      if prev_assigns.tailing? do
        maybe_cancel_tailing_timer(socket)
        SearchQueryExecutor.maybe_cancel_query(stoken)

        socket
        |> assign(:tailing?, false)
        |> push_patch_with_params(%{tailing?: false, querystring: prev_assigns.querystring})
      else
        socket
      end

    {:noreply, socket}
  end

  def handle_event("start_live_search" = ev, _, %{assigns: prev_assigns} = socket) do
    %{source: source} = prev_assigns
    log_lv_received_event(ev, source)

    socket =
      if prev_assigns.tailing? do
        socket
      else
        socket
        |> assign(:tailing?, true)
        |> assign_new_search_with_qs(
          %{querystring: prev_assigns.querystring, tailing?: true},
          source.bq_table_schema
        )
      end

    {:noreply, socket}
  end

  def handle_event("pause_live_search" = ev, _, %{assigns: prev_assigns} = socket) do
    %{source: %{token: stoken} = source} = prev_assigns
    log_lv_received_event(ev, source)

    socket =
      if prev_assigns.tailing? do
        maybe_cancel_tailing_timer(socket)
        SearchQueryExecutor.maybe_cancel_query(stoken)

        socket
        |> assign(:tailing?, false)
      else
        socket
      end

    {:noreply, socket}
  end

  def handle_event("resume_live_search" = ev, _, %{assigns: prev_assigns} = socket) do
    %{source: %{token: stoken} = source} = prev_assigns
    log_lv_received_event(ev, source)

    socket =
      if prev_assigns.tailing? do
        socket =
          socket
          |> assign(:tailing?, true)

        SearchQueryExecutor.maybe_execute_events_query(stoken, socket.assigns)

        socket
      else
        socket
      end

    {:noreply, socket}
  end

  def handle_event("form_update" = ev, %{"search" => search}, %{assigns: prev_assigns} = socket) do
    source = prev_assigns.source
    log_lv_received_event(ev, source)

    new_qs = search["querystring"]
    new_chart_agg = String.to_existing_atom(search["chart_aggregate"])
    new_chart_period = String.to_existing_atom(search["chart_period"])

    socket = assign(socket, :querystring, new_qs)

    prev_chart_rule =
      Lql.Utils.get_chart_rule(prev_assigns.lql_rules) || Lql.Utils.default_chart_rule()

    socket =
      if new_chart_agg != prev_chart_rule.aggregate or
           new_chart_period != prev_chart_rule.period do
        lql_rules =
          prev_assigns.lql_rules
          |> Lql.Utils.update_chart_rule(
            Lql.Utils.default_chart_rule(),
            %{
              aggregate: new_chart_agg,
              period: new_chart_period
            }
          )

        qs = Lql.encode!(lql_rules)

        socket
        |> assign(:querystring, qs)
        |> assign(:lql_rules, lql_rules)
        |> assign(:log_aggregates, [])
        |> assign(:loading, true)
        |> assign(:chart_loading?, true)
        |> push_patch_with_params(%{querystring: qs, tailing?: prev_assigns.tailing?})
      else
        socket
      end

    {:noreply, socket}
  end

  def handle_event("timestamp_and_chart_update" = ev, params, %{assigns: assigns} = socket) do
    log_lv_received_event(ev, socket.assigns.source)

    ts_qs = Map.get(params, "querystring")
    period = Map.get(params, "period")

    maybe_cancel_tailing_timer(socket)
    SearchQueryExecutor.maybe_cancel_query(socket.assigns.source.token)

    {:ok, ts_rules} = Lql.decode(ts_qs, assigns.source.bq_table_schema)

    lql_list =
      assigns.lql_rules
      |> Lql.Utils.update_timestamp_rules(ts_rules)

    lql_list =
      if period do
        Lql.Utils.put_chart_period(lql_list, String.to_existing_atom(period))
      else
        lql_list
      end

    qs = Lql.encode!(lql_list)

    socket =
      socket
      |> assign(:tailing?, false)
      |> assign(:lql_rules, lql_list)
      |> assign(:querystring, qs)
      |> push_patch_with_params(%{querystring: qs, tailing?: assigns.tailing?})

    {:noreply, socket}
  end

  def handle_event(
        "start_search" = ev,
        %{"search" => %{"querystring" => qs}},
        %{assigns: prev_assigns} = socket
      ) do
    %{id: _sid, token: stoken} = prev_assigns.source
    log_lv_received_event(ev, prev_assigns.source)
    bq_table_schema = prev_assigns.source.bq_table_schema

    maybe_cancel_tailing_timer(socket)
    SearchQueryExecutor.maybe_cancel_query(stoken)

    socket =
      assign_new_search_with_qs(
        socket,
        %{querystring: qs, tailing?: prev_assigns.tailing?},
        bq_table_schema
      )

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

  def handle_event("save_search" = ev, _, socket) do
    %{source: source, querystring: qs, lql_rules: lql_rules, tailing?: tailing?, user: user} =
      socket.assigns

    log_lv_received_event(ev, source)

    %Plans.Plan{limit_saved_search_limit: limit} = Plans.get_plan_by_user(user)

    if Enum.count(source.saved_searches) < limit do
      case SavedSearches.save_by_user(qs, lql_rules, source, tailing?) do
        {:ok, _saved_search} ->
          socket =
            assign_notifications(socket, :warning, "Search saved!")
            |> assign(:source, Sources.Cache.get_source_for_lv_param(source.id))

          socket.assigns.notifications

          {:noreply, socket}

        {:error, changeset} ->
          {message, _} = changeset.errors[:querystring]
          socket = assign_notifications(socket, :warning, "Save search error: #{message}")
          {:noreply, socket}
      end
    else
      socket =
        assign_notifications(
          socket,
          :warning,
          "You've reached your saved search limit for this source. Delete one or upgrade first!"
        )

      {:noreply, socket}
    end
  end

  def handle_event("reset_search", _, %{assigns: assigns} = socket) do
    lql_rules = Lql.decode!(@default_qs, assigns.source.bq_table_schema)
    qs = Lql.encode!(lql_rules)

    socket =
      socket
      |> assign(:querystring, qs)
      |> assign(:lql_rules, lql_rules)

    {:noreply, socket}
  end

  def handle_info({:search_result, %{aggregates: _aggs} = search_result}, socket) do
    log_lv_received_event("search_result", socket.assigns.source)

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
          &BqSchemaHelpers.format_timestamp(&1, timezone)
        )
      end)

    if socket.assigns.tailing? do
      log_lv(socket.assigns.source, "is scheduling tail aggregate")

      %ChartRule{period: period} =
        socket.assigns.lql_rules
        |> Enum.find(fn x -> Map.has_key?(x, :period) end)

      Process.send_after(self(), :schedule_tail_agg, period_to_ms(period))
    end

    socket =
      socket
      |> assign(:log_aggregates, log_aggregates)
      |> assign(:search_op_log_aggregates, search_result.aggregates)
      |> assign(:chart_loading?, false)

    {:noreply, socket}
  end

  def handle_info({:search_result, %{events: _events} = search_result}, socket) do
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
        match?({:warning, _}, search_result.events.status) ->
          {:warning, message} = search_result.events.status
          message

        true ->
          warning_message(socket.assigns, search_result)
      end

    socket = if warning, do: assign_notifications(socket, :warning, warning), else: socket

    socket =
      socket
      |> assign(:log_events, search_result.events.rows)
      |> assign(:search_result, search_result.events)
      |> assign(:search_op_error, nil)
      |> assign(:search_op_log_events, search_result.events)
      |> assign(:tailing_timer, tailing_timer)
      |> assign(:loading, false)
      |> assign(:tailing_initial?, false)
      |> assign(:last_query_completed_at, Timex.now())

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
      SearchQueryExecutor.maybe_execute_events_query(assigns.source.token, assigns)
    end

    {:noreply, socket}
  end

  def handle_info(:schedule_tail_agg = msg, %{assigns: assigns} = socket) do
    if socket.assigns.tailing? do
      log_lv_received_info(msg, assigns.source)
      SearchQueryExecutor.maybe_execute_agg_query(assigns.source.token, assigns)
    end

    {:noreply, socket}
  end

  defp assign_new_search_with_qs(socket, params, bq_table_schema) do
    %{querystring: qs, tailing?: tailing?} = params

    with {:ok, lql_rules} <- Lql.decode(qs, bq_table_schema) do
      lql_rules = Lql.Utils.put_new_chart_rule(lql_rules, Lql.Utils.default_chart_rule())
      qs = Lql.encode!(lql_rules)

      socket
      |> assign(:loading, true)
      |> assign(:chart_loading, true)
      |> assign(:tailing_initial?, true)
      |> assign_notifications(:warning, nil)
      |> assign_notifications(:error, nil)
      |> assign(:lql_rules, lql_rules)
      |> assign(:querystring, qs)
      |> push_patch_with_params(%{querystring: qs, tailing?: tailing?})
    else
      {:error, error} ->
        error_socket(socket, error)
    end
  end

  defp push_patch_with_params(socket, %{querystring: querystring, tailing?: tailing?}) do
    path =
      Routes.live_path(socket, __MODULE__, socket.assigns.source.id, %{
        querystring: querystring,
        tailing?: tailing?
      })

    push_patch(socket, to: path, replace: false)
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

  defp kickoff_queries(source_token, assigns) when is_atom(source_token) do
    SearchQueryExecutor.maybe_execute_events_query(source_token, assigns)
    SearchQueryExecutor.maybe_execute_agg_query(source_token, assigns)
  end

  defp period_to_ms(:second), do: :timer.seconds(1)
  defp period_to_ms(:minute), do: :timer.minutes(1)
  defp period_to_ms(:hour), do: :timer.hours(1)
  defp period_to_ms(:day), do: :timer.hours(24)

  defp error_socket(socket, error) do
    socket
    |> assign(:log_events, [])
    |> assign(:log_aggregates, [])
    |> assign(:tailing?, false)
    |> assign(:loading, false)
    |> assign(:chart_loading?, false)
    |> assign_notifications(:error, error)
  end
end
