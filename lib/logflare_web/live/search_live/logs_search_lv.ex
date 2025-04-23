defmodule LogflareWeb.Source.SearchLV do
  @moduledoc """
  Handles all user interactions with the source logs search
  """
  use LogflareWeb, :live_view

  alias Logflare.Billing
  alias Logflare.Logs.SearchQueryExecutor
  alias Logflare.Lql
  alias Logflare.Lql.ChartRule
  alias Logflare.SavedSearches
  alias Logflare.Sources
  alias Logflare.TeamUsers
  alias Logflare.User
  alias Logflare.Users
  alias LogflareWeb.Helpers.BqSchema, as: BqSchemaHelpers
  alias LogflareWeb.Router.Helpers, as: Routes
  alias LogflareWeb.SearchView

  import LogflareWeb.Helpers.BqSchema
  import LogflareWeb.ModalLiveHelpers
  import Logflare.Lql.Utils
  alias Logflare.DateTimeUtils
  alias LogflareWeb.Search

  import Logflare.Logs.Search.Utils
  import LogflareWeb.SearchLV.Utils

  require Logger
  embed_templates "templates/*"

  @tail_search_interval 500
  @user_idle_interval :timer.minutes(5)
  @default_qs "c:count(*) c:group_by(t::minute)"

  def mount(
        %{"source_id" => source_id} = params,
        %{"user_id" => user_id} = session,
        socket
      ) do
    source = Sources.get_source_for_lv_param(source_id)
    socket = assign(socket, :source, source)
    user = Users.get_by_and_preload(id: user_id)

    team_user =
      if team_user_id = session["team_user_id"] do
        TeamUsers.get_team_user_and_preload(team_user_id)
      end

    tailing? =
      if source.disable_tailing, do: false, else: Map.get(params, "tailing?", "true") == "true"

    socket
    |> assign(
      source: source,
      user: user,
      team_user: team_user,
      search_tip: gen_search_tip(),
      user_local_timezone: Map.get(params, "tz", "Etc/UTC"),
      user_timezone_from_connect_params: nil,
      use_local_time: true,
      # loading states
      loading: true,
      chart_loading: true,
      # tailing states
      tailing_initial?: true,
      tailing_timer: nil,
      tailing?: tailing?,
      # search states
      search_op: nil,
      search_op_error: nil,
      search_op_log_events: nil,
      search_op_log_aggregates: nil,
      chart_aggregate_enabled?: nil,
      user_idle_interval: @user_idle_interval,
      show_modal: nil,
      last_query_completed_at: nil,
      uri_params: nil,
      uri: nil,
      lql_rules: [],
      querystring: Map.get(params, "querystring", @default_qs),
      force_query: Map.get(params, "force", "false") == "true",
      search_history: [],
      search_form: to_form(%{}, as: :search)
    )
    |> then(fn socket ->
      if connected?(socket) do
        user_tz = Map.get(get_connect_params(socket), "user_timezone")
        socket = assign(socket, :user_timezone_from_connect_params, user_tz)
        assign_new_user_timezone(socket, team_user, user)
      else
        socket
      end
    end)
    |> then(fn socket ->
      if user && (user.admin or source.user_id == user.id) do
        {:ok, socket}
      else
        {:ok, redirect(socket, to: "/")}
      end
    end)
  end

  def handle_params(%{"querystring" => qs} = params, uri, socket) do
    source = socket.assigns.source

    socket =
      socket
      |> assign(:show_modal, false)
      |> assign(uri: URI.parse(uri))
      |> assign(uri_params: params)

    socket =
      if team_user = socket.assigns[:team_user],
        do: assign(socket, :team_user, TeamUsers.get_team_user_and_preload(team_user.id)),
        else: socket

    socket = assign_new_user_timezone(socket, socket.assigns[:team_user], socket.assigns.user)

    socket =
      with {:ok, lql_rules} <- Lql.decode(qs, source.bq_table_schema),
           lql_rules = Lql.Utils.put_new_chart_rule(lql_rules, Lql.Utils.default_chart_rule()),
           {:ok, socket} <- check_suggested_keys(lql_rules, source, socket) do
        qs = Lql.encode!(lql_rules)

        search_op_log_events =
          if socket.assigns.search_op_log_events do
            rows = socket.assigns.search_op_log_events.rows
            events = Enum.map(rows, &Map.put(&1, :is_from_stale_query, true))
            Map.put(socket.assigns.search_op_log_events, :rows, events)
          else
            socket.assigns.search_op_log_events
          end

        socket =
          socket
          |> assign(:loading, true)
          |> assign(:chart_loading, true)
          |> assign(:tailing_initial?, true)
          |> assign(:lql_rules, lql_rules)
          |> assign(:querystring, qs)
          |> assign(:search_op_log_events, search_op_log_events)
          |> assign(chart_aggregate_enabled?: search_agg_controls_enabled?(lql_rules))

        if connected?(socket) do
          kickoff_queries(source.token, socket.assigns)
        end

        socket
      else
        {:error, :required_field_not_found} ->
          error_socket(socket, :required_field_not_found)

        {:error, :suggested_field_not_found} ->
          error_socket(socket, :suggested_field_not_found)

        {:error, error} ->
          socket
          |> assign(:querystring, qs)
          |> error_socket(error)

        {:error, :field_not_found = type, suggested_querystring, error} ->
          socket
          |> assign(:querystring, qs)
          |> error_socket(type, suggested_querystring, error)
      end

    {:noreply, socket}
  end

  def handle_params(%{"source_id" => source}, uri, socket) do
    params = %{"source_id" => source, "querystring" => "", "tailing?" => "true"}
    handle_params(params, uri, socket)
  end

  def handle_params(_params, _uri, socket) do
    source = socket.assigns.source
    socket = assign(socket, :page_title, source.name)
    {:noreply, socket}
  end

  def render(assigns) do
    logs_search(assigns)
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

  def handle_event(direction = ev, _, socket) when direction in ["backwards", "forwards"] do
    log_lv_received_event(ev, socket.assigns.source)
    rules = socket.assigns.lql_rules

    timestamp_rules =
      rules
      |> Lql.Utils.get_ts_filters()
      |> Lql.Utils.get_filter_rules()

    if Enum.empty?(timestamp_rules) do
      socket =
        put_flash(
          socket,
          :error,
          "To jump #{direction} please include a timestamp filter in your query."
        )

      {:noreply, socket}
    else
      timestamp_rules =
        if socket.assigns.use_local_time do
          user_local_timezone = socket.assigns.user_local_timezone
          tz = Timex.Timezone.get(user_local_timezone)

          Enum.map(timestamp_rules, fn
            lql_rule ->
              if Lql.Utils.timestamp_filter_rule_is_shorthand?(lql_rule) do
                Map.replace!(
                  lql_rule,
                  :values,
                  for value <- lql_rule.values do
                    Timex.shift(value, seconds: Timex.diff(value, tz))
                  end
                )
              else
                lql_rule
              end
          end)
        else
          timestamp_rules
        end

      rules = Lql.Utils.update_timestamp_rules(rules, timestamp_rules)
      new_rules = Lql.Utils.jump_timestamp(rules, String.to_atom(direction))
      qs = Lql.encode!(new_rules)

      socket =
        socket
        |> assign(:lql_rules, new_rules)
        |> push_patch_with_params(%{tailing?: false, querystring: qs})

      {:noreply, socket}
    end
  end

  def handle_event("soft_play" = ev, _, %{assigns: %{uri_params: _params}} = socket) do
    log_lv_received_event(ev, socket.assigns.source)

    soft_play(ev, socket)
  end

  def handle_event("soft_pause" = ev, _, %{assigns: %{uri_params: _params}} = socket) do
    log_lv_received_event(ev, socket.assigns.source)

    soft_pause(ev, socket)
  end

  def handle_event("hard_play" = ev, _, socket) do
    log_lv_received_event(ev, socket.assigns.source)

    hard_play(ev, socket)
  end

  def handle_event("form_focus", %{"value" => value}, socket) do
    send(self(), :soft_pause)

    source = socket.assigns.source
    search_history = search_history(value, source)

    {:noreply, assign(socket, :search_history, search_history)}
  end

  def handle_event("form_blur", %{"value" => _value}, socket) do
    :noop

    {:noreply, socket}
  end

  def handle_event("form_update" = ev, %{"search" => search}, %{assigns: prev_assigns} = socket) do
    source = prev_assigns.source
    log_lv_received_event(ev, source)

    new_qs = search["querystring"]
    new_chart_agg = String.to_existing_atom(search["chart_aggregate"])
    new_chart_period = String.to_existing_atom(search["chart_period"])

    search_history = search_history(new_qs, source)

    socket = assign(socket, :querystring, new_qs)

    prev_chart_rule =
      Lql.Utils.get_chart_rule(prev_assigns.lql_rules) || Lql.Utils.default_chart_rule()

    socket =
      if new_chart_agg != prev_chart_rule.aggregate or
           new_chart_period != prev_chart_rule.period do
        lql_rules =
          Lql.Utils.update_chart_rule(
            prev_assigns.lql_rules,
            Lql.Utils.default_chart_rule(),
            %{aggregate: new_chart_agg, period: new_chart_period}
          )

        qs = Lql.encode!(lql_rules)

        socket
        |> assign(:search_history, search_history)
        |> assign(:querystring, qs)
        |> assign(:lql_rules, lql_rules)
        |> assign(:loading, true)
        |> assign(:chart_loading, true)
        |> clear_flash()
        |> push_patch_with_params(%{querystring: qs, tailing?: prev_assigns.tailing?})
      else
        assign(socket, :search_history, search_history)
      end

    {:noreply, socket}
  end

  def handle_event("datetime_update" = ev, params, %{assigns: assigns} = socket) do
    log_lv_received_event(ev, socket.assigns.source)

    ts_qs = Map.get(params, "querystring")
    period = Map.get(params, "period")

    maybe_cancel_tailing_timer(socket)
    SearchQueryExecutor.maybe_cancel_query(socket.assigns.source.token)

    {:ok, ts_rules} = Lql.decode(ts_qs, assigns.source.bq_table_schema)

    lql_list = Lql.Utils.update_timestamp_rules(assigns.lql_rules, ts_rules)

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
      |> push_patch_with_params(%{querystring: qs, tailing?: false})

    {:noreply, socket}
  end

  def handle_event("toggle_local_time", _metadata, socket) do
    source = socket.assigns.source
    maybe_cancel_tailing_timer(socket)
    SearchQueryExecutor.maybe_cancel_query(source.token)

    socket =
      socket
      |> assign(:use_local_time, not socket.assigns.use_local_time)
      |> assign_new_search_with_qs(
        %{querystring: socket.assigns.querystring, tailing?: socket.assigns.tailing?},
        socket.assigns.source.bq_table_schema
      )

    {:noreply, socket}
  end

  def handle_event("save_search" = ev, _, socket) do
    %{
      source: source,
      querystring: qs,
      lql_rules: lql_rules,
      tailing?: tailing?,
      user: user
    } = socket.assigns

    log_lv_received_event(ev, source)

    %Billing.Plan{limit_saved_search_limit: limit} = Billing.get_plan_by_user(user)

    if Enum.count(source.saved_searches) < limit do
      case SavedSearches.save_by_user(qs, lql_rules, source, tailing?) do
        {:ok, _saved_search} ->
          socket =
            socket
            |> put_flash(:info, "Search saved!")
            |> assign(:source, Sources.get_source_for_lv_param(source.id))

          {:noreply, socket}

        {:error, changeset} ->
          {message, _} = changeset.errors[:querystring]
          socket = put_flash(socket, :info, "Save search error: #{message}")
          {:noreply, socket}
      end
    else
      socket =
        put_flash(
          socket,
          :warning,
          "You've reached your saved search limit for this source. Delete one or upgrade first!"
        )

      {:noreply, socket}
    end
  end

  def handle_event("reset_search" = ev, _, socket) do
    log_lv_received_event(ev, socket.assigns.source)
    {:noreply, reset_search(socket)}
  end

  def handle_info(:soft_pause = ev, socket) do
    soft_pause(ev, socket)
  end

  def handle_info(:hard_play = ev, socket) do
    hard_play(ev, socket)
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
        Map.update!(la, "timestamp", &BqSchemaHelpers.format_timestamp(&1, timezone))
      end)

    aggs =
      search_result.aggregates
      |> Map.from_struct()
      |> put_in([:rows], log_aggregates)

    if socket.assigns.tailing? do
      log_lv(socket.assigns.source, "is scheduling tail aggregate")

      %ChartRule{period: period} =
        socket.assigns.lql_rules
        |> Enum.find(fn x -> Map.has_key?(x, :period) end)

      Process.send_after(self(), :schedule_tail_agg, period_to_ms(period))
    end

    socket =
      socket
      |> assign(:chart_loading, false)
      |> assign(:search_op_log_aggregates, aggs)

    {:noreply, socket}
  end

  def handle_info({:search_result, %{events: _events} = search_result}, socket) do
    log_lv_received_event("search_result", socket.assigns.source)

    tailing_timer =
      if socket.assigns.tailing? do
        log_lv(socket.assigns.source, "is scheduling tail search")
        Process.send_after(self(), :schedule_tail_search, @tail_search_interval)
      end

    socket =
      socket
      |> assign(:search_op_error, nil)
      |> assign(:search_op_log_events, search_result.events)
      |> assign(:tailing_timer, tailing_timer)
      |> assign(:loading, false)
      |> assign(:tailing_initial?, false)
      |> assign(:last_query_completed_at, Timex.now())

    socket =
      cond do
        match?({:warning, _}, search_result.events.status) ->
          {:warning, message} = search_result.events.status
          put_flash(socket, :info, message)

        msg = warning_message(socket.assigns, search_result) ->
          put_flash(socket, :warning, msg)

        true ->
          socket
      end

    {:noreply, socket}
  end

  def handle_info({:search_error = msg, search_op}, %{assigns: %{source: source}} = socket) do
    log_lv_received_info(msg, source)

    socket =
      case search_op.error do
        :halted ->
          send(self(), :soft_pause)

          {:halted, halted_message} = search_op.status
          msg = "Search halted: " <> halted_message

          socket
          |> assign(loading: false)
          |> assign(chart_loading: false)
          |> put_flash(:error, msg)

        %Tesla.Env{status: 400} = err ->
          Logger.error("Backend search error for source: #{source.token}",
            error_string: inspect(err),
            source_id: source.token
          )

          send(self(), :soft_pause)

          msg = "Backend error! Retry your query. Please contact support if this continues."

          socket
          |> assign(loading: false)
          |> assign(chart_loading: false)
          |> put_flash(:error, msg)

        err ->
          Logger.error("Backend search error for source: #{source.token}",
            error_string: inspect(err),
            source_id: source.token
          )

          send(self(), :soft_pause)

          socket
          |> assign(loading: false)
          |> assign(chart_loading: false)
      end

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

    # source disable_tailing overrides search tailing
    tailing? = if socket.assigns.source.disable_tailing, do: false, else: tailing?

    case Lql.decode(qs, bq_table_schema) do
      {:ok, lql_rules} ->
        lql_rules = Lql.Utils.put_new_chart_rule(lql_rules, Lql.Utils.default_chart_rule())
        qs = Lql.encode!(lql_rules)

        socket
        |> assign(:loading, true)
        |> assign(:tailing_initial?, true)
        |> clear_flash()
        |> assign(:lql_rules, lql_rules)
        |> assign(:querystring, qs)
        |> push_patch_with_params(%{querystring: qs, tailing?: tailing?})

      {:error, error} ->
        error_socket(socket, error)

      {:error, :field_not_found = type, suggested_querystring, error} ->
        error_socket(socket, type, suggested_querystring, error)
    end
  end

  defp assign_new_user_timezone(socket, team_user, %User{} = user) do
    tz_connect = socket.assigns.user_timezone_from_connect_params

    tz_connect =
      if tz_connect && Timex.Timezone.exists?(tz_connect) do
        tz_connect
      else
        "Etc/UTC"
      end

    tz_param = Map.get(socket.assigns.uri_params || %{}, "tz")

    cond do
      tz_param != nil ->
        socket
        |> assign(:user_local_timezone, tz_param)

      team_user && team_user.preferences ->
        assign(socket, :user_local_timezone, team_user.preferences.timezone)

      team_user && is_nil(team_user.preferences) ->
        {:ok, team_user} =
          Users.update_user_with_preferences(team_user, %{preferences: %{timezone: tz_connect}})

        socket
        |> assign(:team_user, team_user)
        |> assign(:user_local_timezone, tz_connect)
        |> put_flash(
          :info,
          "Your timezone setting for team #{team_user.team.name} sources was set to #{tz_connect}. You can change it using the 'timezone' link in the top menu."
        )

      user.preferences ->
        assign(socket, :user_local_timezone, user.preferences.timezone)

      is_nil(user.preferences) ->
        {:ok, user} =
          Users.update_user_with_preferences(user, %{preferences: %{timezone: tz_connect}})

        socket
        |> assign(:user_local_timezone, tz_connect)
        |> assign(:user, user)
        |> put_flash(
          :info,
          "Your timezone was set to #{tz_connect}. You can change it using the 'timezone' link in the top menu."
        )
    end
    |> then(fn
      %{assigns: %{uri_params: %{"tz" => tz}, user_local_timezone: local_tz}} = socket
      when tz != local_tz ->
        push_patch_with_params(socket, %{"tz" => local_tz})

      %{assigns: %{uri_params: params, user_local_timezone: local_tz}} = socket
      when not is_map_key(params, "tz") and local_tz != "Etc/UTC" ->
        push_patch_with_params(socket, %{"tz" => local_tz})

      _ ->
        socket
    end)
  end

  defp push_patch_with_params(socket, new_params) do
    params = Map.merge(socket.assigns.uri_params || %{}, new_params)

    path =
      Routes.live_path(socket, __MODULE__, socket.assigns.source.id, params)

    push_patch(socket, to: path, replace: false)
  end

  defp warning_message(assigns, search_op) do
    tailing? = assigns.tailing?
    querystring = assigns.querystring
    log_events_empty? = Enum.empty?(search_op.events.rows)

    cond do
      log_events_empty? and not tailing? ->
        "No log events matching your search query."

      log_events_empty? and tailing? ->
        "No log events matching your search query."

      querystring == "" and log_events_empty? and tailing? ->
        "No log events ingested during last 24 hours. Try searching over a longer time period, and clicking the bar chart to drill down."

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
    Logger.info("Kicking off queries for #{source_token}", source_id: source_token)

    SearchQueryExecutor.maybe_execute_events_query(source_token, assigns)
    SearchQueryExecutor.maybe_execute_agg_query(source_token, assigns)
  end

  defp period_to_ms(:second), do: :timer.seconds(1)
  defp period_to_ms(:minute), do: :timer.minutes(1)
  defp period_to_ms(:hour), do: :timer.hours(1)
  defp period_to_ms(:day), do: :timer.hours(24)

  defp error_socket(socket, :field_not_found, suggested_querystring, [head, replace, tail]) do
    path =
      Routes.live_path(
        socket,
        LogflareWeb.Source.SearchLV,
        socket.assigns.source,
        querystring: suggested_querystring,
        tailing?: socket.assigns.tailing?
      )

    replace = link(replace, to: path)

    error = [head, replace, tail]

    socket
    |> assign(querystring: socket.assigns.querystring)
    |> error_socket(error)
  end

  defp error_socket(socket, :required_field_not_found) do
    keys =
      socket.assigns.source.suggested_keys
      |> String.split(",")
      |> Enum.filter(fn key -> String.ends_with?(key, "!") end)
      |> Enum.map(fn key -> String.trim_trailing(key, "!") end)
      |> Enum.join(", ")

    error = [
      "Query does not include required keys.",
      Phoenix.HTML.raw("<br/><code class=\"tw-text-sm\">"),
      keys,
      Phoenix.HTML.raw("</code>")
    ]

    error_socket(socket, error)
  end

  defp error_socket(socket, :suggested_field_not_found) do
    path =
      Routes.live_path(socket, LogflareWeb.Source.SearchLV, socket.assigns.source,
        force: true,
        tailing?: true,
        loading: true,
        chart_loading: true,
        querystring: socket.assigns.querystring
      )

    keys =
      socket.assigns.source.suggested_keys
      |> String.split(",")
      |> Enum.map(fn key -> String.trim_trailing(key, "!") end)
      |> Enum.join(", ")

    error = [
      "Query does not include suggested keys.",
      Phoenix.HTML.raw("<br/><code class=\"tw-text-sm\">"),
      keys,
      Phoenix.HTML.raw("</code><br/>"),
      "Do you want to proceed?",
      link("Click to force query", to: path)
    ]

    error_socket(socket, error)
  end

  defp error_socket(socket, error) do
    socket
    |> assign(:tailing?, false)
    |> assign(:loading, false)
    |> assign(:chart_loading, false)
    |> put_flash(:error, error)
  end

  defp search_history(new_qs, source) do
    search_history = SavedSearches.suggest_saved_searches(new_qs, source.id)

    if Enum.count(search_history) == 1 &&
         hd(search_history).querystring == new_qs,
       do: [],
       else: search_history
  end

  defp soft_play(
         _ev,
         %{assigns: %{uri_params: %{"tailing?" => "false"}}} = socket
       ) do
    {:noreply, socket}
  end

  defp soft_play(_ev, %{assigns: %{source: %_{disable_tailing: true}}} = socket) do
    {:noreply, error_socket(socket, "Tailing is disabled for this source")}
  end

  defp soft_play(ev, %{assigns: prev_assigns} = socket) do
    %{source: %{token: stoken} = source} = prev_assigns
    log_lv_received_event(ev, source)

    kickoff_queries(stoken, socket.assigns)

    socket =
      socket
      |> assign(:tailing?, true)

    {:noreply, socket}
  end

  defp soft_pause(
         _ev,
         %{assigns: %{uri_params: %{"tailing?" => "false"}}} = socket
       ) do
    {:noreply, socket}
  end

  defp soft_pause(ev, %{assigns: prev_assigns} = socket) do
    %{source: %{token: stoken} = source} = prev_assigns
    log_lv_received_event(ev, source)

    maybe_cancel_tailing_timer(socket)
    SearchQueryExecutor.maybe_cancel_query(stoken)

    socket =
      socket
      |> assign(:tailing?, false)

    {:noreply, socket}
  end

  defp hard_play(
         _ev,
         %{assigns: %{source: %_{disable_tailing: true}}} = socket
       ) do
    {:noreply, error_socket(socket, "Tailing is disabled for this source")}
  end

  defp hard_play(ev, %{assigns: prev_assigns} = socket) do
    %{source: %{token: stoken} = source} = prev_assigns
    log_lv_received_event(ev, source)

    kickoff_queries(stoken, socket.assigns)

    socket =
      socket
      |> assign(:tailing?, true)
      |> push_patch_with_params(%{
        querystring: prev_assigns.querystring,
        tailing?: true
      })

    {:noreply, socket}
  end

  defp reset_search(%{assigns: assigns} = socket) do
    lql_rules = Lql.decode!(@default_qs, assigns.source.bq_table_schema)
    qs = Lql.encode!(lql_rules)

    socket
    |> assign(:querystring, qs)
    |> assign(:lql_rules, lql_rules)
  end

  defp check_suggested_keys(_lql_rules, _source, %{assigns: %{force_query: true}} = socket),
    do: {:ok, socket}

  defp check_suggested_keys(_lql_rules, %{suggested_keys: ""}, socket),
    do: {:ok, socket}

  defp check_suggested_keys(_lql_rules, %{suggested_keys: nil}, socket),
    do: {:ok, socket}

  defp check_suggested_keys(
         lql_rules,
         %{suggested_keys: suggested_keys},
         %{assigns: %{force_query: false}} = socket
       ) do
    {required, suggested} =
      suggested_keys
      |> String.split(",")
      |> Enum.map(fn
        "m." <> suggested_field -> "metadata." <> suggested_field
        suggested_field -> suggested_field
      end)
      |> Enum.split_with(fn suggested_field -> String.ends_with?(suggested_field, "!") end)

    suggested_present =
      Enum.all?(suggested, fn suggested_field ->
        Enum.find(lql_rules, fn %{path: path} -> path == suggested_field end)
      end)

    required_present =
      Enum.all?(required, fn required_field ->
        trimmed = String.trim_trailing(required_field, "!")
        Enum.find(lql_rules, fn %{path: path} -> path == trimmed end)
      end)

    cond do
      !required_present -> {:error, :required_field_not_found}
      !suggested_present -> {:error, :suggested_field_not_found}
      true -> {:ok, socket}
    end
  end
end
