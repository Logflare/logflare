defmodule LogflareWeb.Source.SearchLV do
  @moduledoc """
  Handles all user interactions with the source logs search.
  """
  use LogflareWeb, :live_view

  import Logflare.Lql.Rules
  import LogflareWeb.ModalLiveHelpers
  import LogflareWeb.SearchLV.Utils

  alias Logflare.Billing
  alias Logflare.Logs.SearchQueryExecutor
  alias Logflare.Logs.SearchUtils
  alias Logflare.Lql
  alias Logflare.Lql.Rules
  alias Logflare.Lql.Rules.ChartRule
  alias Logflare.SavedSearches
  alias Logflare.SourceSchemas
  alias Logflare.Sources
  alias Logflare.TeamUsers
  alias Logflare.User
  alias Logflare.Users
  alias LogflareWeb.AuthLive
  alias LogflareWeb.Helpers.BqSchema, as: BqSchemaHelpers
  alias LogflareWeb.Router.Helpers, as: Routes
  alias LogflareWeb.SearchLive.FormComponents
  alias LogflareWeb.SearchLive.SubheadComponents
  alias LogflareWeb.SearchLive.LogEventComponents
  alias LogflareWeb.Utils
  alias Logflare.Sources.Source.BigQuery.SchemaBuilder
  alias Logflare.Utils.Chart, as: ChartUtils

  require Logger

  @tail_search_interval 1000
  @user_idle_interval :timer.minutes(2)
  @default_qs "c:count(*) c:group_by(t::minute)"

  on_mount LogflareWeb.AuthLive

  def mount(%{"source_id" => source_id} = params, _session, socket) do
    %{assigns: %{user: user, team_user: team_user}} = socket
    effective_user = team_user || user

    source =
      if user && user.admin do
        Sources.get_source_for_lv_param(source_id)
      else
        Sources.get_by_user_access(effective_user, source_id)
        |> maybe_preload_source_for_lv()
      end

    case source do
      nil ->
        {:ok,
         socket
         |> put_flash(:error, "Source not found")
         |> redirect(to: ~p"/dashboard" |> Utils.with_team_param(socket.assigns[:team]))}

      source ->
        {:ok, mount_with_source(socket, source, params, effective_user)}
    end
  end

  defp mount_with_source(socket, source, %{"t" => _team_id} = params, _effective_user) do
    %{assigns: %{user: user, team_user: team_user}} = socket

    tailing? =
      if source.disable_tailing,
        do: false,
        else: Map.get(params, "tailing?", "true") == "true"

    {:ok, executor_pid} = SearchQueryExecutor.start_link(source: source)

    socket
    |> assign(
      executor_pid: executor_pid,
      source: source,
      search_tip: SearchUtils.gen_search_tip(),
      user_timezone_from_connect_params: nil,
      search_timezone: Map.get(params, "tz", "Etc/UTC"),
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
    |> maybe_assign_user_timezone(team_user, user)
  end

  defp mount_with_source(socket, source, params, effective_user) do
    socket = AuthLive.assign_context_by_resource(socket, source, effective_user.email)
    params = Map.put(params, "t", socket.assigns.team.id)

    mount_with_source(socket, source, params, effective_user)
  end

  defp maybe_assign_user_timezone(socket, team_user, user) do
    if connected?(socket) do
      user_tz = Map.get(get_connect_params(socket), "user_timezone")

      socket
      |> assign(:user_timezone_from_connect_params, user_tz)
      |> assign_new_user_timezone(team_user, user)
    else
      socket
    end
  end

  defp maybe_preload_source_for_lv(nil), do: nil

  defp maybe_preload_source_for_lv(source) do
    source
    |> Sources.preload_defaults()
    |> Sources.put_bq_table_id()
    |> Sources.put_bq_dataset_id()
  end

  def handle_params(%{"fields" => fields, "querystring" => qs} = params, _uri, socket)
      when is_map(fields) do
    source = socket.assigns.source

    bq_table_schema =
      source
      |> get_bigquery_schema()

    qs = append_fields_rules(qs, fields, bq_table_schema)

    params =
      params
      |> Map.delete("fields")
      |> Map.delete("source_id")
      |> Map.put("querystring", qs)

    path =
      Routes.live_path(socket, __MODULE__, source.id, params)

    {:noreply, push_patch(socket, to: path, replace: true)}
  end

  def handle_params(%{"querystring" => qs} = params, uri, socket) do
    source = socket.assigns.source

    tailing? = Map.get(params, "tailing?", "true") != "false" and socket.assigns.tailing?

    socket =
      socket
      |> assign(:show_modal, false)
      |> assign(:tailing?, tailing?)
      |> assign(uri: URI.parse(uri))
      |> assign(uri_params: params)

    socket =
      if team_user = socket.assigns[:team_user],
        do: assign(socket, :team_user, TeamUsers.get_team_user_and_preload(team_user.id)),
        else: socket

    socket = assign_new_user_timezone(socket, socket.assigns[:team_user], socket.assigns.user)

    socket =
      with {:ok, lql_rules} <-
             Lql.decode(qs, get_bigquery_schema(source)),
           lql_rules = Rules.put_new_chart_rule(lql_rules, Rules.default_chart_rule()),
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
    ~H"""
    <%= if @show_modal do %>
      {live_modal(@modal.body.module_or_template,
        id: @modal.body.id,
        title: @modal.body.title,
        user: @user,
        params: @modal.params,
        view: @modal.body[:view],
        source: @source,
        search_op_log_events: @search_op_log_events,
        search_op_log_aggregates: @search_op_log_aggregates,
        search_op_error: @search_op_error,
        team_user: @team_user,
        team: @team,
        close: @modal.body[:close],
        return_to: @modal.body.return_to
      )}
    <% end %>
    <.subheader>
      <:path>
        ~/logs/<.team_link team={@team} href={~p"/sources/#{@source}"} class="text-primary">{@source.name}</.team_link>/search
      </:path>
      <SubheadComponents.subhead_actions user={@user} source={@source} search_timezone={@search_timezone} search_op_error={@search_op_error} search_op_log_events={@search_op_log_events} search_op_log_aggregates={@search_op_log_aggregates} />
    </.subheader>
    <div class="container source-logs-search-container console-text">
      <div id="logs-list-container">
        <LogEventComponents.empty_result_list :if={not @loading} search_op_log_events={@search_op_log_events} search_op_log_aggregates={@search_op_log_aggregates} />
        <LogEventComponents.results_list search_op={@search_op} search_op_log_events={@search_op_log_events} last_query_completed_at={@last_query_completed_at} search_timezone={@search_timezone} loading={@loading} tailing?={@tailing?} querystring={@querystring} />
      </div>
      <div>
        {live_react_component(
          "Components.LogEventsChart",
          %{
            data: if(@search_op_log_aggregates, do: @search_op_log_aggregates.rows, else: []),
            loading: @chart_loading,
            display_timezone: @search_timezone || "Etc/UTC",
            chart_period: get_chart_period(@lql_rules, "minute"),
            chart_data_shape_id:
              if(@search_op_log_aggregates,
                do: @search_op_log_aggregates.chart_data_shape_id,
                else: nil
              )
          },
          id: "log-events-chart"
        )}
      </div>
      <FormComponents.search_controls
        search_form={@search_form}
        querystring={@querystring}
        search_history={@search_history}
        search_timezone={@search_timezone}
        loading={@loading}
        tailing?={@tailing?}
        uri_params={@uri_params}
        lql_rules={@lql_rules}
        user={@user}
        has_results?={[@search_op_log_events, @search_op_log_aggregates] |> Enum.any?()}
        source={@source}
        last_query_completed_at={@last_query_completed_at}
      />
      <div id="user-idle" phx-click="user_idle" class="d-none" data-user-idle-interval={@user_idle_interval}></div>
    </div>
    """
  end

  def handle_event(
        "results-action-change",
        %{"remember_timezone" => "true", "search_timezone" => timezone},
        socket
      ) do
    %{user: user, search_timezone: search_timezone} = socket.assigns

    preferences =
      user.preferences
      |> Map.from_struct()
      |> Map.put(:timezone, timezone)

    socket =
      case Users.update_user_with_preferences(user, %{preferences: preferences}) do
        {:ok, user} ->
          socket
          |> assign(user: user)
          |> put_flash(:info, "Timezone preference saved")

        {:error, _changeset} ->
          socket
          |> put_flash(:error, "Timezone preference could not be saved")
      end

    if timezone == search_timezone do
      {:noreply, socket}
    else
      handle_event(
        "results-action-change",
        %{"search_timezone" => timezone},
        socket
      )
    end
  end

  def handle_event("results-action-change", %{"search_timezone" => tz}, socket) do
    maybe_cancel_tailing_timer(socket)
    SearchQueryExecutor.cancel_query(socket.assigns.executor_pid)

    socket =
      socket
      |> assign(:search_timezone, tz)
      |> assign_new_search_with_qs(
        %{querystring: socket.assigns.querystring, tailing?: socket.assigns.tailing?},
        get_bigquery_schema(socket.assigns.source)
      )

    {:noreply, socket |> assign(:timezone, tz)}
  end

  def handle_event(
        "start_search",
        %{"search" => %{"querystring" => qs}} = params,
        %{assigns: prev_assigns} = socket
      ) do
    bq_table_schema = get_bigquery_schema(socket.assigns.source)

    maybe_cancel_tailing_timer(socket)
    SearchQueryExecutor.cancel_query(socket.assigns.executor_pid)

    qs =
      append_fields_rules(
        qs,
        Map.get(params, "fields", %{}),
        bq_table_schema
      )

    socket =
      assign_new_search_with_qs(
        socket,
        %{querystring: qs, tailing?: prev_assigns.tailing?},
        bq_table_schema
      )

    {:noreply, socket}
  end

  def handle_event(direction, _, socket) when direction in ["backwards", "forwards"] do
    rules = socket.assigns.lql_rules

    timestamp_rules =
      rules
      |> Lql.Rules.get_timestamp_filters()
      |> Lql.Rules.get_filter_rules()

    if Enum.empty?(timestamp_rules) do
      socket =
        put_flash(
          socket,
          :error,
          "To jump #{direction} please include a timestamp filter in your query."
        )

      {:noreply, socket}
    else
      timestamp_rules = adjust_timestamp_rules(timestamp_rules, socket.assigns.search_timezone)

      rules = Lql.Rules.update_timestamp_rules(rules, timestamp_rules)
      new_rules = Lql.Rules.jump_timestamp(rules, String.to_atom(direction))
      qs = Lql.encode!(new_rules)

      socket =
        socket
        |> assign(:lql_rules, new_rules)
        |> push_patch_with_params(%{tailing?: false, querystring: qs})

      {:noreply, socket}
    end
  end

  def handle_event("soft_play" = ev, _, %{assigns: %{uri_params: _params}} = socket) do
    soft_play(ev, socket)
  end

  def handle_event("soft_pause" = ev, _, %{assigns: %{uri_params: _params}} = socket) do
    soft_pause(ev, socket)
  end

  def handle_event("hard_play" = ev, _, socket) do
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

  def handle_event("form_update" = _ev, %{"search" => search}, %{assigns: prev_assigns} = socket) do
    source = prev_assigns.source

    new_qs = search["querystring"]
    new_chart_agg = String.to_existing_atom(search["chart_aggregate"])
    new_chart_period = String.to_existing_atom(search["chart_period"])

    search_history = search_history(new_qs, source)

    socket = assign(socket, :querystring, new_qs)

    prev_chart_rule =
      Lql.Rules.get_chart_rule(prev_assigns.lql_rules) || Lql.Rules.default_chart_rule()

    socket =
      if new_chart_agg != prev_chart_rule.aggregate or
           new_chart_period != prev_chart_rule.period do
        lql_rules =
          Lql.Rules.update_chart_rule(
            prev_assigns.lql_rules,
            Lql.Rules.default_chart_rule(),
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

  def handle_event("datetime_update" = _ev, params, %{assigns: assigns} = socket) do
    ts_qs = Map.get(params, "querystring")
    period = Map.get(params, "period")

    maybe_cancel_tailing_timer(socket)
    SearchQueryExecutor.cancel_query(socket.assigns.executor_pid)

    {:ok, ts_rules} =
      Lql.decode(ts_qs, get_bigquery_schema(assigns.source))

    lql_list = Lql.Rules.update_timestamp_rules(assigns.lql_rules, ts_rules)

    lql_list =
      if period do
        Lql.Rules.put_chart_period(lql_list, String.to_existing_atom(period))
      else
        lql_list |> maybe_adjust_chart_period()
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

  def handle_event("save_search" = _ev, _, socket) do
    %{
      source: source,
      querystring: qs,
      lql_rules: lql_rules,
      tailing?: tailing?,
      user: user
    } = socket.assigns

    %Billing.Plan{limit_saved_search_limit: limit} = Billing.get_plan_by_user(user)

    if SavedSearches.Cache.list_saved_searches_by_source(source.id) |> length() < limit do
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

  def handle_event("reset_search", _, socket) do
    {:noreply, reset_search(socket)}
  end

  def handle_event(
        "create_new",
        %{"kind" => kind, "resource" => resource},
        %{assigns: %{source: source} = assigns} = socket
      ) do
    search_op =
      if kind == "aggregates",
        do: assigns.search_op_log_aggregates,
        else: assigns.search_op_log_events

    sql =
      Utils.sql_params_to_sql(search_op.sql_string, search_op.sql_params)
      |> Utils.replace_table_with_source_name(source)

    destination =
      case resource do
        "endpoint" ->
          ~p"/endpoints/new?#{%{query: sql, name: source.name}}"

        "alert" ->
          ~p"/alerts/new?#{%{query: sql, name: source.name}}"

        "query" ->
          ~p"/query?#{%{q: sql}}"
      end
      |> LogflareWeb.Utils.with_team_param(assigns[:team])

    {:noreply, push_navigate(socket, to: destination)}
  end

  defp append_fields_rules(qs, recommended_fields, schema) do
    with true <- is_map(recommended_fields),
         {:ok, lql_rules} <- Lql.decode(qs, schema) do
      recommended_filter_rules =
        recommended_fields
        |> Enum.reject(fn {_path, value} -> is_nil(value) or String.trim(value) == "" end)
        |> Enum.map(fn {path, value} ->
          Lql.Rules.FilterRule.build(path: path, operator: :=, value: value)
        end)

      lql_rules =
        Enum.reduce(recommended_filter_rules, lql_rules, fn recommended_filter_rule, rules ->
          Rules.upsert_filter_rule_by_path(rules, recommended_filter_rule)
        end)

      Lql.encode!(lql_rules)
    else
      _ -> qs
    end
  end

  def handle_info(:soft_pause = ev, socket) do
    soft_pause(ev, socket)
  end

  def handle_info(:hard_play = ev, socket) do
    hard_play(ev, socket)
  end

  def handle_info({:search_result, %{aggregates: _aggs} = search_result}, socket) do
    log_aggregates =
      search_result.aggregates.rows
      |> Enum.reverse()
      |> Enum.map(fn la ->
        Map.update!(
          la,
          "timestamp",
          &BqSchemaHelpers.format_timestamp(&1, socket.assigns.search_timezone)
        )
      end)

    aggs =
      search_result.aggregates
      |> Map.from_struct()
      |> put_in([:rows], log_aggregates)

    if socket.assigns.tailing? do
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

  def handle_info({:search_result, %{events: events_op} = search_result}, socket) do
    tailing_timer =
      if socket.assigns.tailing? do
        Process.send_after(self(), :schedule_tail_search, @tail_search_interval)
      end

    socket =
      socket
      |> assign(:search_op, events_op)
      |> assign(:search_op_error, nil)
      |> assign(:search_op_log_events, search_result.events)
      |> assign(:tailing_timer, tailing_timer)
      |> assign(:loading, false)
      |> assign(:tailing_initial?, false)
      |> assign(:last_query_completed_at, DateTime.utc_now())

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

  def handle_info({:search_error, search_op}, %{assigns: %{source: source}} = socket) do
    socket =
      case search_op.error do
        :halted ->
          send(self(), :soft_pause)

          socket
          |> assign(loading: false)
          |> assign(chart_loading: false)
          |> put_halt_flash_message(search_op)

        %Tesla.Env{status: 400} = err ->
          Logger.error("Backend search error for source: #{source.token}",
            error_string: inspect(err),
            source_id: source.token
          )

          send(self(), :soft_pause)

          socket
          |> assign(loading: false)
          |> assign(chart_loading: false)
          |> put_flash_query_error(err)

        err ->
          Logger.error("Backend search error for source: #{source.token}",
            error_string: inspect(err),
            source_id: source.token
          )

          send(self(), :soft_pause)

          socket
          |> assign(loading: false)
          |> assign(chart_loading: false)
          |> put_flash_query_error(err)
      end

    {:noreply, socket}
  end

  def handle_info(:schedule_tail_search, %{assigns: assigns} = socket) do
    if socket.assigns.tailing? do
      SearchQueryExecutor.query(assigns.executor_pid, assigns)
    end

    {:noreply, socket}
  end

  def handle_info(:schedule_tail_agg, %{assigns: assigns} = socket) do
    if socket.assigns.tailing? do
      SearchQueryExecutor.query_agg(assigns.executor_pid, assigns)
    end

    {:noreply, socket}
  end

  def handle_info({:set_flash, {type, message}}, socket) do
    {:noreply, put_flash(socket, type, message)}
  end

  defp assign_new_search_with_qs(socket, params, bq_table_schema) do
    %{querystring: qs, tailing?: tailing?} = params

    # source disable_tailing overrides search tailing
    tailing? = if socket.assigns.source.disable_tailing, do: false, else: tailing?

    tz = socket.assigns.search_timezone

    case Lql.decode(qs, bq_table_schema) do
      {:ok, lql_rules} ->
        lql_rules = Lql.Rules.put_new_chart_rule(lql_rules, Lql.Rules.default_chart_rule())
        qs = Lql.encode!(lql_rules)

        socket
        |> assign(:loading, true)
        |> assign(:tailing_initial?, true)
        |> clear_flash()
        |> assign(:lql_rules, lql_rules)
        |> assign(:querystring, qs)
        |> push_patch_with_params(%{querystring: qs, tz: tz, tailing?: tailing?})

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
        |> assign(:search_timezone, tz_param)

      team_user && team_user.preferences ->
        socket
        |> assign(:search_timezone, team_user.preferences.timezone)

      team_user && is_nil(team_user.preferences) ->
        {:ok, team_user} =
          Users.update_user_with_preferences(team_user, %{preferences: %{timezone: tz_connect}})

        socket
        |> assign(:team_user, team_user)
        |> assign(:search_timezone, tz_connect)
        |> put_flash(
          :info,
          "Your timezone setting for team #{team_user.team.name} sources was set to #{tz_connect}. You can change it using the 'timezone' link in the top menu."
        )

      user.preferences ->
        socket
        |> assign(:search_timezone, user.preferences.timezone)

      is_nil(user.preferences) ->
        {:ok, user} =
          Users.update_user_with_preferences(user, %{preferences: %{timezone: tz_connect}})

        socket
        |> assign(:search_timezone, tz_connect)
        |> assign(:display_timezone, tz_connect)
        |> assign(:user, user)
        |> put_flash(
          :info,
          "Your timezone was set to #{tz_connect}. You can change it using the 'timezone' dropdown in the top menu."
        )
    end
    |> then(fn
      %{assigns: %{uri_params: %{"tz" => tz}, search_timezone: local_tz}} = socket
      when tz != local_tz ->
        push_patch_with_params(socket, %{"tz" => local_tz})

      %{assigns: %{uri_params: params, search_timezone: local_tz}} = socket
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

  defp adjust_timestamp_rules(timestamp_rules, search_timezone) do
    tz = Timex.Timezone.get(search_timezone)

    Enum.map(timestamp_rules, fn
      lql_rule ->
        if Lql.Rules.timestamp_filter_rule_is_shorthand?(lql_rule) do
          Map.replace!(lql_rule, :values, shift_timestamps(lql_rule.values, tz))
        else
          lql_rule
        end
    end)
  end

  defp shift_timestamps(timestamps, timezone) do
    Enum.map(timestamps, &Timex.shift(&1, seconds: Timex.diff(&1, timezone)))
  end

  defp kickoff_queries(source_token, assigns) when is_atom(source_token) do
    Logger.debug("Kicking off queries for #{source_token}", source_id: source_token)

    if assigns do
      SearchQueryExecutor.query(assigns.executor_pid, assigns)
      SearchQueryExecutor.query_agg(assigns.executor_pid, assigns)
    end
  end

  @doc """
  Adjusts the chart period in LQL rules when the number of chart ticks would exceed the maximum or be zero.

  Does nothing if the ChartRule is already valid, or if the ChartRule is not present.
  """

  @spec maybe_adjust_chart_period(Lql.Rules.lql_rules()) :: Lql.Rules.lql_rules()
  def maybe_adjust_chart_period(lql_rules) do
    max_ticks = Logflare.Logs.SearchOperations.max_chart_ticks()

    with [%Lql.Rules.FilterRule{values: [min_ts, max_ts]}] <-
           Lql.Rules.get_timestamp_filters(lql_rules),
         %ChartRule{} = chart_rule <- Lql.Rules.get_chart_rule(lql_rules),
         false <-
           ChartUtils.get_number_of_chart_ticks(min_ts, max_ts, chart_rule.period) in [
             1..max_ticks
           ] do
      period = ChartUtils.calculate_minimum_required_period(min_ts, max_ts, max_ticks)

      Lql.Rules.put_chart_period(lql_rules, period)
    else
      _ -> lql_rules
    end
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

  defp soft_play(_ev, %{assigns: prev_assigns} = socket) do
    %{source: %{token: stoken} = _source} = prev_assigns

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

  defp soft_pause(_ev, %{assigns: %{source: _source, executor_pid: executor_pid}} = socket) do
    maybe_cancel_tailing_timer(socket)
    SearchQueryExecutor.cancel_query(executor_pid)

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

  defp hard_play(_ev, %{assigns: prev_assigns} = socket) do
    %{source: %{token: stoken} = _source} = prev_assigns

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
    lql_rules =
      Lql.decode!(@default_qs, get_bigquery_schema(assigns.source))

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
        Enum.find(lql_rules, fn
          %{path: path} -> path == suggested_field
          _ -> false
        end)
      end)

    required_present =
      Enum.all?(required, fn required_field ->
        trimmed = String.trim_trailing(required_field, "!")

        Enum.find(lql_rules, fn
          %{path: path} -> path == trimmed
          _ -> false
        end)
      end)

    cond do
      !required_present -> {:error, :required_field_not_found}
      !suggested_present -> {:error, :suggested_field_not_found}
      true -> {:ok, socket}
    end
  end

  defp put_flash_query_error(socket, %Tesla.Env{status: 400} = response) do
    case Jason.decode(response.body) do
      {:ok, %{"error" => %{"message" => "Query exceeded limit for bytes billed:" <> rest}}} ->
        [limit | _] = String.split(rest, ".")

        {size, units} = limit |> String.trim() |> String.to_integer() |> Utils.humanize_bytes()

        socket
        |> put_flash(
          :error,
          "Query halted: total bytes processed for this query is expected to be greater than #{round(size)} #{units}"
        )

      _ ->
        put_flash_query_error(socket, nil)
    end
  end

  defp put_flash_query_error(socket, _response) do
    socket
    |> put_flash(
      :error,
      "Backend error! Retry your query. Please contact support if this continues."
    )
  end

  defp put_halt_flash_message(socket, search_op) do
    {:halted, message} = search_op.status

    msg =
      if message =~ ~r/longer chart aggregation period.$/ or message =~ ~r/shorter chart period.$/ do
        quickfix = quickfix_chart_period(socket.assigns.uri, search_op.lql_rules)
        ["Search halted: ", message, quickfix]
      else
        "Search halted: " <> message
      end

    put_flash(socket, :error, msg)
  end

  defp quickfix_chart_period(uri, lql_rules) do
    adjusted_lql = lql_rules |> maybe_adjust_chart_period()

    params =
      uri.query
      |> URI.decode_query()
      |> Map.put("querystring", Lql.encode!(adjusted_lql))

    adjusted_period = Lql.Rules.get_chart_period(adjusted_lql)

    link("Set chart period to #{adjusted_period}",
      to: %{uri | query: URI.encode_query(params)},
      class: "tw-block tw-pt-3"
    )
  end

  defp get_bigquery_schema(source) do
    if source_schema = SourceSchemas.Cache.get_source_schema_by(source_id: source.id) do
      source_schema.bigquery_schema
    else
      SchemaBuilder.initial_table_schema()
    end
  end
end
