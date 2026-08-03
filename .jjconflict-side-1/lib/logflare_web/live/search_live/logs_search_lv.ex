defmodule LogflareWeb.Source.SearchLV do
  @moduledoc """
  Handles all user interactions with the source logs search.
  """

  use LogflareWeb, :live_view

  import Logflare.Lql.Rules
  import LogflareWeb.ModalLiveHelpers
  import LogflareWeb.SearchLV.Utils

  alias Logflare.Backends.QueryError
  alias Logflare.Billing
  alias Logflare.Logs.EventPage
  alias Logflare.Logs.SearchOperation
  alias Logflare.Logs.SearchQueryExecutor
  alias Logflare.Logs.SearchOperations
  alias Logflare.Logs.SearchOperations.Helpers, as: SearchOperationHelpers
  alias Logflare.Logs.SearchUtils
  alias Logflare.Lql
  alias Logflare.Lql.Rules
  alias Logflare.Lql.Rules.ChartRule
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.SavedSearches
  alias Logflare.SourceSchemas
  alias Logflare.Sources
  alias Logflare.TeamUsers
  alias Logflare.User
  alias Logflare.Users
  alias LogflareWeb.Helpers.BqSchema, as: BqSchemaHelpers
  alias LogflareWeb.QueryErrorHelpers
  alias LogflareWeb.Router.Helpers, as: Routes
  alias LogflareWeb.SearchLive.EventPagination
  alias LogflareWeb.SearchLive.FormComponents
  alias LogflareWeb.SearchLive.SubheadComponents
  alias LogflareWeb.SearchLive.LogEventComponents
  alias LogflareWeb.Utils
  alias Logflare.Utils.Chart, as: ChartUtils

  require Logger

  @log_event_stream_limit 5_000
  @tail_search_interval 1000
  @user_idle_interval :timer.minutes(2)
  @timeout_search_error_message "Query timed out: Try restricting the timestamp range or adding more filtering to your query."

  on_mount LogflareWeb.AuthLive
  on_mount {LogflareWeb.AuthLive, :ensure_team_param}

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
        {:ok, mount_with_source(socket, source, params)}
    end
  end

  defp mount_with_source(socket, source, params) do
    %{assigns: %{user: user, team_user: team_user}} = socket

    tailing? =
      if source.disable_tailing,
        do: false,
        else: Map.get(params, "tailing?", "true") == "true"

    {:ok, executor_pid} = SearchQueryExecutor.start_link(source: source)

    flat_map = SourceSchemas.source_schema_flatmap_or_default(source)

    socket
    |> assign(
      executor_pid: executor_pid,
      source: source,
      source_schema_flat_map: flat_map,
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
      resume_tailing_after_modal?: false,
      # search states
      search_op_error: nil,
      search_op_log_events: nil,
      search_op_log_aggregates: nil,
      user_idle_interval: @user_idle_interval,
      show_modal: nil,
      last_query_completed_at: nil,
      uri_params: nil,
      uri: nil,
      lql_rules: [],
      saved_searches: saved_searches(source),
      force_query: Map.get(params, "force", "false") == "true"
    )
    |> reset_event_pagination()
    |> stream_configure(:log_events, dom_id: &log_event_dom_id/1)
    |> stream(:log_events, [])
    |> maybe_assign_user_timezone(team_user, user)
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

    schema_flatmap = SourceSchemas.source_schema_flatmap_or_default(source)

    qs = append_fields_rules(qs, fields, schema_flatmap)

    params =
      params
      |> Map.delete("fields")
      |> Map.delete("source_id")
      |> Map.put("querystring", qs)

    path =
      Routes.live_path(socket, __MODULE__, source.id, params)
      |> Utils.with_team_param(socket.assigns[:team])

    {:noreply, push_patch(socket, to: path, replace: true)}
  end

  def handle_params(
        %{"querystring" => qs} = params,
        uri,
        %{
          assigns: %{
            event_pagination: %{
              range_extension: expected_querystring
            }
          }
        } = socket
      )
      when is_binary(expected_querystring) do
    if qs == expected_querystring do
      {:noreply,
       socket
       |> update_event_pagination(&EventPagination.clear_range_extension/1)
       |> assign(uri: URI.parse(uri), uri_params: params, querystring: qs)}
    else
      socket =
        socket
        |> update_event_pagination(&EventPagination.clear_range_extension/1)

      handle_params(params, uri, socket)
    end
  end

  def handle_params(%{"querystring" => qs} = params, uri, socket) do
    source = socket.assigns.source

    qs = querystring_or_default(qs, source)

    tailing? = Map.get(params, "tailing?", "true") != "false" and socket.assigns.tailing?

    socket =
      socket
      |> assign(:show_modal, false)
      |> assign(:tailing?, tailing?)
      |> assign(uri: URI.parse(uri))
      |> assign(uri_params: params)
      |> assign(querystring: qs)

    socket =
      if team_user = socket.assigns[:team_user],
        do: assign(socket, :team_user, TeamUsers.get_team_user_and_preload(team_user.id)),
        else: socket

    socket = assign_new_user_timezone(socket, socket.assigns[:team_user], socket.assigns.user)

    socket =
      with {:ok, lql_rules} <-
             Lql.decode(qs, SourceSchemas.source_schema_flatmap_or_default(source)),
           lql_rules = Rules.put_new_chart_rule(lql_rules, Rules.default_chart_rule()),
           {:ok, socket} <- check_suggested_keys(lql_rules, source, socket) do
        qs = Lql.encode!(lql_rules)

        socket =
          socket
          |> assign(:loading, true)
          |> assign(:chart_loading, true)
          |> reset_event_pagination()
          |> assign(:tailing_initial?, true)
          |> assign(:lql_rules, lql_rules)
          |> assign(:querystring, qs)

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
        source_schema_flat_map: @source_schema_flat_map,
        search_op_log_events: @search_op_log_events,
        search_op_log_aggregates: @search_op_log_aggregates,
        search_op_error: @search_op_error,
        team_user: @team_user,
        team: @team,
        lql: @querystring,
        querystring: @querystring,
        search_timezone: @search_timezone,
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
        <LogEventComponents.results_list
          search_op_log_events={@search_op_log_events}
          search_op_log_aggregates={@search_op_log_aggregates}
          log_events={@streams.log_events}
          search_timezone={@search_timezone}
          loading={@loading}
          tailing?={@tailing?}
          pagination_buttons={event_pagination_buttons(@event_pagination, @pagination_cursors, @tailing?, @loading, @lql_rules, @search_timezone)}
          source_schema_flat_map={@source_schema_flat_map}
        />
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
        querystring={@querystring}
        saved_searches={@saved_searches}
        loading={@loading}
        tailing?={@tailing?}
        uri_params={@uri_params}
        lql_rules={@lql_rules}
        user={@user}
        has_results?={[@search_op_log_events, @search_op_log_aggregates] |> Enum.any?()}
        source={@source}
        last_query_completed_at={@last_query_completed_at}
        lql_schema_flat_map={lql_schema_flat_map(@source)}
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
        SourceSchemas.source_schema_flatmap_or_default(socket.assigns.source)
      )

    {:noreply, socket |> assign(:timezone, tz)}
  end

  def handle_event(
        "start_search",
        %{"querystring" => qs} = params,
        %{assigns: prev_assigns} = socket
      ) do
    schema_flatmap = SourceSchemas.source_schema_flatmap_or_default(socket.assigns.source)

    maybe_cancel_tailing_timer(socket)
    SearchQueryExecutor.cancel_query(socket.assigns.executor_pid)

    qs = append_fields_rules(qs, Map.get(params, "fields", %{}), schema_flatmap)

    socket =
      socket
      |> assign_new_search_with_qs(
        %{querystring: qs, tailing?: prev_assigns.tailing?},
        schema_flatmap
      )

    {:noreply, socket}
  end

  def handle_event(
        "load_events",
        %{
          "intent" => intent,
          "cursor-id" => cursor_id,
          "cursor-timestamp" => cursor_timestamp
        },
        %{assigns: %{loading: false, tailing?: false}} = socket
      ) do
    with {:ok, {intent, cursor}} <- event_page_request(intent, cursor_id, cursor_timestamp),
         :ok <-
           SearchQueryExecutor.query(
             socket.assigns.executor_pid,
             socket.assigns,
             intent,
             cursor
           ) do
      {:noreply, socket}
    else
      _ -> {:noreply, socket}
    end
  end

  def handle_event("load_events", _params, socket), do: {:noreply, socket}

  def handle_event(direction, _, socket) when direction in ["backwards", "forwards"] do
    rules = socket.assigns.lql_rules

    timestamp_rules =
      rules
      |> Rules.get_timestamp_filters()
      |> Rules.get_filter_rules()

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

      rules = Rules.update_timestamp_rules(rules, timestamp_rules)
      new_rules = Rules.jump_timestamp(rules, String.to_atom(direction))
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

  def handle_event("open_log_event_modal", _, socket) do
    resume_tailing? = socket.assigns.tailing?

    socket =
      socket
      |> assign(:resume_tailing_after_modal?, resume_tailing?)
      |> pause_tailing()

    {:noreply, socket}
  end

  def handle_event("close_log_event_modal", _, socket) do
    socket =
      if socket.assigns.resume_tailing_after_modal? do
        resume_tailing(socket)
      else
        socket
      end

    {:noreply, assign(socket, :resume_tailing_after_modal?, false)}
  end

  def handle_event("hard_play" = ev, _, socket) do
    hard_play(ev, socket)
  end

  def handle_event("form_focus", %{"value" => _value}, socket) do
    send(self(), :soft_pause)
    {:noreply, socket}
  end

  def handle_event("form_blur", %{"value" => _value}, socket) do
    :noop

    {:noreply, socket}
  end

  def handle_event("querystring_changed", %{"querystring" => _qs}, socket) do
    {:noreply, socket}
  end

  def handle_event(
        "chart_controls_update",
        %{"chart_aggregate" => new_chart_agg, "chart_period" => new_chart_period},
        socket
      ) do
    socket =
      maybe_update_chart_controls(
        socket,
        String.to_existing_atom(new_chart_agg),
        String.to_existing_atom(new_chart_period)
      )

    {:noreply, socket}
  end

  def handle_event("datetime_update" = _ev, params, %{assigns: assigns} = socket) do
    ts_qs = Map.get(params, "querystring")
    period = Map.get(params, "period")

    maybe_cancel_tailing_timer(socket)
    SearchQueryExecutor.cancel_query(socket.assigns.executor_pid)

    {:ok, ts_rules} =
      Lql.decode(ts_qs, SourceSchemas.source_schema_flatmap_or_default(assigns.source))

    lql_list = Rules.update_timestamp_rules(assigns.lql_rules, ts_rules)

    lql_list =
      if period do
        Rules.put_chart_period(lql_list, String.to_existing_atom(period))
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
          saved_searches =
            [qs | saved_searches(source)]
            |> Enum.uniq()
            |> Enum.sort_by(&String.downcase/1)

          socket =
            socket
            |> put_flash(:info, "Search saved!")
            |> assign(:source, Sources.get_source_for_lv_param(source.id))
            |> assign(:saved_searches, saved_searches)

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
        |> Enum.map(fn {path, value} -> {path, trim_field_value(value)} end)
        |> Enum.reject(fn {_path, value} -> value == "" end)
        |> Enum.map(fn {path, value} ->
          FilterRule.build(path: path, operator: :=, value: value)
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

  defp trim_field_value(value) when is_binary(value), do: String.trim(value)
  defp trim_field_value(_value), do: ""

  defp maybe_update_chart_controls(socket, new_chart_agg, new_chart_period) do
    prev_chart_rule =
      Lql.Rules.get_chart_rule(socket.assigns.lql_rules) || Lql.Rules.default_chart_rule()

    if new_chart_agg != prev_chart_rule.aggregate or
         new_chart_period != prev_chart_rule.period do
      lql_rules =
        Lql.Rules.update_chart_rule(
          socket.assigns.lql_rules,
          Lql.Rules.default_chart_rule(),
          %{aggregate: new_chart_agg, period: new_chart_period}
        )

      qs = Lql.encode!(lql_rules)

      socket
      |> assign(:querystring, qs)
      |> assign(:lql_rules, lql_rules)
      |> assign(:loading, true)
      |> assign(:chart_loading, true)
      |> reset_event_pagination()
      |> clear_flash()
      |> push_patch_with_params(%{querystring: qs, tailing?: socket.assigns.tailing?})
    else
      socket
    end
  end

  defp update_event_pagination(socket, update) do
    assign(socket, :event_pagination, update.(socket.assigns.event_pagination))
  end

  defp reset_event_pagination(socket) do
    socket
    |> assign(:event_pagination, EventPagination.new())
    |> assign(:pagination_cursors, %{previous: nil, next: nil})
  end

  defp put_pagination_cursors(socket, event_page, :initial) do
    assign(socket, :pagination_cursors, %{
      previous: event_page.cursor,
      next: event_page.next_cursor
    })
  end

  defp put_pagination_cursors(socket, %EventPage{next_cursor: nil}, :tail), do: socket

  defp put_pagination_cursors(socket, event_page, :tail) do
    update(socket, :pagination_cursors, &%{&1 | next: event_page.next_cursor})
  end

  defp put_pagination_cursors(socket, event_page, intent) when intent in [:previous, :next] do
    update(socket, :pagination_cursors, &Map.put(&1, intent, event_page.cursor))
  end

  defp event_page_request("previous", cursor_id, cursor_timestamp) do
    build_event_page_request(:previous, cursor_id, cursor_timestamp)
  end

  defp event_page_request("next", cursor_id, cursor_timestamp) do
    build_event_page_request(:next, cursor_id, cursor_timestamp)
  end

  defp event_page_request(_intent, _cursor_id, _cursor_timestamp), do: :error

  defp build_event_page_request(intent, cursor_id, cursor_timestamp)
       when is_binary(cursor_id) and is_binary(cursor_timestamp) do
    with {cursor_timestamp, ""} <- Integer.parse(cursor_timestamp),
         cursor = %{id: cursor_id, timestamp: cursor_timestamp},
         true <- EventPage.valid_cursor?(cursor) do
      {:ok, {intent, cursor}}
    else
      _ -> :error
    end
  end

  defp build_event_page_request(_intent, _cursor_id, _cursor_timestamp), do: :error

  defp event_pagination_buttons(
         pagination,
         cursors,
         tailing?,
         loading?,
         lql_rules,
         search_timezone
       ) do
    EventPagination.buttons(pagination,
      cursors: cursors,
      tailing?: tailing?,
      loading?: loading?,
      next_available?: next_page_available?(pagination, cursors.next, lql_rules, search_timezone)
    )
  end

  defp next_page_available?(
         %EventPagination{next_exhausted?: false},
         cursor,
         lql_rules,
         search_timezone
       )
       when not is_nil(cursor) do
    timestamp_filters =
      %SearchOperation{
        chart_data_shape_id: nil,
        lql_ts_filters: Rules.get_timestamp_filters(lql_rules),
        partition_by: :timestamp,
        querystring: "",
        search_timezone: search_timezone,
        tailing?: false
      }
      |> SearchOperations.apply_local_timestamp_correction()
      |> Map.fetch!(:lql_ts_filters)

    %{max: range_end} =
      SearchOperationHelpers.get_min_max_filter_timestamps(timestamp_filters, :second)

    range_end =
      case range_end do
        %DateTime{} = datetime -> datetime
        %NaiveDateTime{} = datetime -> DateTime.from_naive!(datetime, "Etc/UTC")
      end

    DateTime.compare(range_end, DateTime.utc_now()) in [:lt, :eq]
  end

  defp next_page_available?(_pagination, _cursor, _lql_rules, _search_timezone), do: false

  defp put_event_page(socket, rows, :previous) do
    socket
    |> stream(:log_events, rows, at: -1)
  end

  defp put_event_page(socket, rows, :next) do
    socket
    |> stream(:log_events, Enum.reverse(rows), at: 0)
  end

  defp put_search_events(socket, rows)
       when socket.assigns.tailing? and not socket.assigns.tailing_initial? do
    rows
    |> Enum.with_index()
    |> Enum.reduce(socket, fn {row, index}, socket ->
      stream_insert(socket, :log_events, row, at: index, limit: @log_event_stream_limit)
    end)
  end

  defp put_search_events(socket, rows) do
    stream(socket, :log_events, rows, reset: true)
  end

  defp apply_event_page_result(socket, %EventPage{} = event_page) do
    socket
    |> assign(:search_op_error, nil)
    |> assign(:last_query_completed_at, DateTime.utc_now())
    |> apply_event_page_result(event_page, event_page.request.intent)
  end

  defp apply_event_page_result(
         socket,
         %EventPage{events: events_op} = event_page,
         :initial
       ) do
    if socket.assigns.tailing_initial? do
      apply_initial_event_page_result(socket, event_page, events_op)
    else
      apply_tail_event_page_result(socket, event_page)
    end
  end

  defp apply_event_page_result(socket, event_page, intent) when intent in [:previous, :next] do
    if event_page.rows != [] do
      extend_timestamp_range(socket, event_page)
    else
      put_event_page_result(socket, event_page, intent)
    end
  end

  defp apply_initial_event_page_result(socket, event_page, events_op) do
    tailing_timer =
      if socket.assigns.tailing? do
        Process.send_after(self(), :schedule_tail_search, @tail_search_interval)
      end

    socket =
      socket
      |> reset_event_pagination()
      |> put_search_events(event_page.rows)
      |> update_event_pagination(&EventPagination.complete_initial(&1, event_page))
      |> put_pagination_cursors(event_page, :initial)
      |> assign(:search_op_log_events, events_op)
      |> assign(:tailing_timer, tailing_timer)
      |> assign(:loading, false)
      |> assign(:tailing_initial?, false)

    if match?({:warning, _}, events_op.status) do
      {:warning, message} = events_op.status
      put_flash(socket, :info, message)
    else
      socket
    end
  end

  defp apply_tail_event_page_result(socket, event_page) do
    tailing_timer =
      if socket.assigns.tailing? do
        Process.send_after(self(), :schedule_tail_search, @tail_search_interval)
      end

    socket
    |> put_search_events(event_page.rows)
    |> update_event_pagination(&EventPagination.complete_tail(&1, event_page))
    |> put_pagination_cursors(event_page, :tail)
    |> assign(:tailing_timer, tailing_timer)
    |> assign(:loading, false)
  end

  defp extend_timestamp_range(
         socket,
         %EventPage{request: %{intent: :previous}} = event_page
       ) do
    event = List.last(event_page.rows)
    timezone = socket.assigns.search_timezone
    event_timestamp = event_timestamp(event, timezone)

    lql_rules =
      socket.assigns.lql_rules
      |> adjust_timestamp_rules(timezone)

    case Rules.effective_timestamp_range(lql_rules) do
      %{min: range_start} ->
        if NaiveDateTime.compare(event_timestamp, range_start) == :lt do
          lql_rules =
            lql_rules
            |> Rules.extend_timestamp_range(:previous, event_timestamp)
            |> maybe_adjust_chart_period()

          push_timestamp_range_extension(socket, event_page, lql_rules)
        else
          put_event_page_result(socket, event_page, :previous)
        end

      _ ->
        put_event_page_result(socket, event_page, :previous)
    end
  end

  defp extend_timestamp_range(socket, %EventPage{request: %{intent: :next}} = event_page) do
    event = List.first(event_page.rows)
    timezone = socket.assigns.search_timezone
    event_timestamp = event_timestamp(event, timezone)

    lql_rules =
      socket.assigns.lql_rules
      |> adjust_timestamp_rules(timezone)

    case Rules.effective_timestamp_range(lql_rules) do
      %{max: range_end} ->
        if NaiveDateTime.compare(event_timestamp, range_end) == :gt do
          lql_rules =
            lql_rules
            |> Rules.extend_timestamp_range(:next, event_timestamp)
            |> maybe_adjust_chart_period()

          push_timestamp_range_extension(socket, event_page, lql_rules)
        else
          put_event_page_result(socket, event_page, :next)
        end

      _ ->
        put_event_page_result(socket, event_page, :next)
    end
  end

  defp put_event_page_result(socket, event_page, intent) when intent in [:previous, :next] do
    socket
    |> put_event_page(event_page.rows, intent)
    |> update_event_pagination(&EventPagination.complete_page(&1, event_page, intent))
    |> put_pagination_cursors(event_page, intent)
  end

  defp push_timestamp_range_extension(socket, event_page, lql_rules) do
    querystring = Lql.encode!(lql_rules)

    socket =
      socket
      |> assign(:lql_rules, lql_rules)
      |> assign(:querystring, querystring)
      |> assign(:chart_loading, true)
      |> put_event_page_result(event_page, event_page.request.intent)
      |> update_event_pagination(&EventPagination.mark_range_extension(&1, querystring))

    SearchQueryExecutor.query_agg(socket.assigns.executor_pid, socket.assigns)

    push_patch_with_params(socket, %{querystring: querystring, tailing?: false})
  end

  defp event_timestamp(event, timezone) do
    event.body["timestamp"]
    |> DateTime.from_unix!(:microsecond)
    |> DateTime.shift_zone!(timezone)
    |> DateTime.to_naive()
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

  def handle_info({:search_result, %{event_page: %EventPage{} = event_page}}, socket) do
    {:noreply, apply_event_page_result(socket, event_page)}
  end

  def handle_info(
        {:search_error, %{event_page_request: %{intent: intent}} = search_op},
        socket
      )
      when intent in [:previous, :next] do
    {:noreply, put_flash_query_error(socket, search_op.error)}
  end

  def handle_info({:search_error, search_op}, socket) do
    socket =
      case search_op.error do
        :halted ->
          send(self(), :soft_pause)

          socket
          |> assign(loading: false)
          |> assign(chart_loading: false)
          |> put_halt_flash_message(search_op)

        err ->
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

  def handle_info({:put_flash, type, message}, socket) do
    {:noreply, put_flash(socket, type, message)}
  end

  defp assign_new_search_with_qs(socket, params, schema_flatmap) do
    %{querystring: qs, tailing?: tailing?} = params

    # source disable_tailing overrides search tailing
    tailing? = if socket.assigns.source.disable_tailing, do: false, else: tailing?

    tz = socket.assigns.search_timezone

    case Lql.decode(qs, schema_flatmap) do
      {:ok, lql_rules} ->
        lql_rules = Rules.put_new_chart_rule(lql_rules, Rules.default_chart_rule())
        qs = Lql.encode!(lql_rules)

        socket
        |> assign(:loading, true)
        |> reset_event_pagination()
        |> stream(:log_events, [], reset: true)
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
      |> Utils.with_team_param(socket.assigns[:team])

    push_patch(socket, to: path, replace: false)
  end

  defp adjust_timestamp_rules(timestamp_rules, search_timezone) do
    case DateTime.now(search_timezone) do
      {:ok, _datetime} -> do_adjust_timestamp_rules(timestamp_rules, search_timezone)
      {:error, _reason} -> timestamp_rules
    end
  end

  defp do_adjust_timestamp_rules(timestamp_rules, timezone) do
    Enum.map(timestamp_rules, fn lql_rule ->
      case lql_rule do
        %{path: "timestamp", modifiers: %{timestamp_origin: :absolute}} ->
          %{
            lql_rule
            | value: shift_timestamp(lql_rule.value, timezone),
              values: shift_timestamps(lql_rule.values, timezone)
          }

        _ ->
          lql_rule
      end
    end)
  end

  @spec shift_timestamps([term()] | nil, Calendar.time_zone()) :: [term()] | nil
  defp shift_timestamps(nil, _timezone), do: nil

  defp shift_timestamps(timestamps, timezone) do
    Enum.map(timestamps, &shift_timestamp(&1, timezone))
  end

  @spec shift_timestamp(term(), Calendar.time_zone()) :: term()
  defp shift_timestamp(nil, _timezone), do: nil

  defp shift_timestamp(timestamp, timezone) when is_integer(timestamp) do
    timestamp
    |> DateTime.from_unix!(:microsecond)
    |> shift_timestamp(timezone)
  end

  defp shift_timestamp(%NaiveDateTime{} = timestamp, timezone) do
    timestamp
    |> DateTime.from_naive!("Etc/UTC")
    |> shift_timestamp(timezone)
  end

  defp shift_timestamp(%DateTime{} = timestamp, timezone) do
    timestamp
    |> DateTime.shift_zone!(timezone)
    |> DateTime.to_naive()
  end

  defp shift_timestamp(timestamp, _timezone), do: timestamp

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

  @spec maybe_adjust_chart_period(Rules.lql_rules()) :: Rules.lql_rules()
  def maybe_adjust_chart_period(lql_rules) do
    max_ticks = SearchOperations.max_chart_ticks()

    with [%FilterRule{values: [min_ts, max_ts]}] <-
           Rules.get_timestamp_filters(lql_rules),
         %ChartRule{} = chart_rule <- Rules.get_chart_rule(lql_rules),
         false <-
           ChartUtils.get_number_of_chart_ticks(min_ts, max_ts, chart_rule.period) in [
             1..max_ticks
           ] do
      period = ChartUtils.calculate_minimum_required_period(min_ts, max_ts, max_ticks)

      Rules.put_chart_period(lql_rules, period)
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
      |> Utils.with_team_param(socket.assigns[:team])

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
      |> Utils.with_team_param(socket.assigns[:team])

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

  defp soft_play(
         _ev,
         %{assigns: %{uri_params: %{"tailing?" => "false"}}} = socket
       ) do
    {:noreply, socket}
  end

  defp soft_play(_ev, %{assigns: %{source: %_{disable_tailing: true}}} = socket) do
    {:noreply, error_socket(socket, "Tailing is disabled for this source")}
  end

  defp soft_play(_ev, socket), do: {:noreply, resume_tailing(socket)}

  defp soft_pause(
         _ev,
         %{assigns: %{uri_params: %{"tailing?" => "false"}}} = socket
       ) do
    {:noreply, socket}
  end

  defp soft_pause(_ev, socket), do: {:noreply, pause_tailing(socket)}

  defp pause_tailing(%{assigns: %{tailing?: false}} = socket), do: socket

  defp pause_tailing(%{assigns: %{executor_pid: executor_pid}} = socket) do
    maybe_cancel_tailing_timer(socket)
    SearchQueryExecutor.cancel_query(executor_pid)

    socket
    |> assign(:tailing?, false)
  end

  defp resume_tailing(socket) do
    kickoff_queries(socket.assigns.source.token, socket.assigns)

    socket
    |> assign(:tailing?, true)
    |> reset_event_pagination()
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
      |> reset_event_pagination()
      |> push_patch_with_params(%{
        querystring: prev_assigns.querystring,
        tailing?: true
      })

    {:noreply, socket}
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

  defp put_flash_query_error(socket, %QueryError{} = error) do
    put_flash(socket, :error, query_error_flash_message(error))
  end

  defp put_flash_query_error(socket, _response) do
    put_flash(socket, :error, "Query halted: " <> QueryErrorHelpers.generic_query_error_message())
  end

  @spec query_error_flash_message(QueryError.t()) :: String.t()
  defp query_error_flash_message(%QueryError{} = error) do
    case QueryErrorHelpers.timeout_query_error?(error) do
      true -> @timeout_search_error_message
      false -> "Query halted: " <> QueryErrorHelpers.query_error_message(error)
    end
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

    adjusted_period = Rules.get_chart_period(adjusted_lql)

    link("Set chart period to #{adjusted_period}",
      to: %{uri | query: URI.encode_query(params)},
      class: "tw-block tw-pt-3"
    )
  end

  @spec log_event_dom_id(Logflare.LogEvent.t()) :: String.t()
  defp log_event_dom_id(%Logflare.LogEvent{id: id, body: %{"timestamp" => timestamp}}) do
    "log-events-#{id}-#{timestamp}"
  end

  @spec querystring_or_default(String.t(), Logflare.Sources.Source.t()) :: String.t()
  defp querystring_or_default("", source), do: source.default_search_lql || ""
  defp querystring_or_default(qs, _source), do: qs

  @spec lql_schema_flat_map(Logflare.Sources.Source.t()) :: map()
  defp lql_schema_flat_map(source) do
    case SourceSchemas.Cache.get_source_schema_by(source_id: source.id) do
      %{schema_flat_map: flat_map} when is_map(flat_map) ->
        flat_map

      _ ->
        %{}
    end
  end

  defp saved_searches(source) do
    source.id
    |> SavedSearches.Cache.list_saved_searches_by_source()
    |> Enum.map(& &1.querystring)
  end
end
