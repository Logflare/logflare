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
  alias Logflare.Logs.SearchOperation
  alias Logflare.Logs.SearchQueryExecutor
  alias Logflare.Logs.SearchOperations
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
  alias LogflareWeb.SearchLive.FormComponents
  alias LogflareWeb.SearchLive.SubheadComponents
  alias LogflareWeb.SearchLive.LogEventComponents
  alias LogflareWeb.Utils
  alias Logflare.Utils.Chart, as: ChartUtils

  require Logger

  @log_event_stream_limit 5_000
  @tail_search_interval 1000
  @user_idle_interval :timer.minutes(2)

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
      event_page_loading: nil,
      event_page_cursors: %{previous: nil, next: nil},
      next_events_exhausted?: false,
      range_extension_patch: nil,
      # tailing states
      tailing_initial?: true,
      tailing_timer: nil,
      tailing?: tailing?,
      resume_tailing_after_context?: false,
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
      saved_searches: saved_searches(source),
      force_query: Map.get(params, "force", "false") == "true"
    )
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
            range_extension_patch: %{
              querystring: expected_querystring,
              rows: rows,
              direction: direction
            }
          }
        } = socket
      )
      when is_binary(expected_querystring) do
    if qs == expected_querystring do
      {:noreply,
       socket
       |> put_event_page(rows, direction)
       |> assign(:event_page_loading, nil)
       |> assign(:range_extension_patch, nil)
       |> assign(uri: URI.parse(uri), uri_params: params, querystring: qs)}
    else
      socket =
        socket
        |> assign(:event_page_loading, nil)
        |> assign(:range_extension_patch, nil)

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
          search_op={@search_op}
          search_op_log_events={@search_op_log_events}
          search_op_log_aggregates={@search_op_log_aggregates}
          log_events={@streams.log_events}
          last_query_completed_at={@last_query_completed_at}
          search_timezone={@search_timezone}
          loading={@loading}
          pagination_available?={bounded_event_pagination?(@tailing?, @lql_rules)}
          unbounded_pagination?={unbounded_event_pagination?(@tailing?, @lql_rules)}
          event_page_loading={@event_page_loading}
          next_events_exhausted?={@next_events_exhausted?}
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
        %{"intent" => intent},
        %{assigns: %{loading: false, event_page_loading: nil, tailing?: false}} = socket
      ) do
    with {:ok, intent} <- event_page_intent(intent),
         {:ok, cursor} <- event_page_cursor(socket.assigns, intent),
         :ok <- event_page_available(socket.assigns, intent),
         :ok <-
           SearchQueryExecutor.query_page(
             socket.assigns.executor_pid,
             socket.assigns,
             intent,
             cursor
           ) do
      maybe_cancel_tailing_timer(socket)

      {:noreply, assign(socket, :event_page_loading, intent)}
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

  def handle_event("open_event_context", _, socket) do
    resume_tailing? = socket.assigns.tailing?

    socket =
      socket
      |> assign(:resume_tailing_after_context?, resume_tailing?)
      |> pause_tailing()

    {:noreply, socket}
  end

  def handle_event("close_event_context", _, socket) do
    socket =
      if socket.assigns.resume_tailing_after_context? do
        resume_tailing(socket)
      else
        socket
      end

    {:noreply, assign(socket, :resume_tailing_after_context?, false)}
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
        |> Enum.reject(fn {_path, value} -> is_nil(value) or String.trim(value) == "" end)
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

  defp reset_event_pagination(socket) do
    socket
    |> assign(:event_page_loading, nil)
    |> assign(:event_page_cursors, %{previous: nil, next: nil})
    |> assign(:range_extension_patch, nil)
    |> assign(:next_events_exhausted?, false)
  end

  defp event_page_intent("within_range"), do: {:ok, :within_range}
  defp event_page_intent("extend_previous"), do: {:ok, :extend_previous}
  defp event_page_intent("extend_next"), do: {:ok, :extend_next}
  defp event_page_intent(_intent), do: :error

  @spec event_page_cursor(map(), SearchOperation.event_page_intent()) ::
          {:ok, SearchOperation.event_cursor() | nil}
  defp event_page_cursor(%{event_page_cursors: cursors}, :extend_next),
    do: {:ok, cursors.next}

  defp event_page_cursor(%{event_page_cursors: cursors}, intent)
       when intent in [:within_range, :extend_previous],
       do: {:ok, cursors.previous}

  defp event_page_available(
         %{search_op_log_events: %{has_more_events?: true}},
         :within_range
       ),
       do: :ok

  defp event_page_available(
         %{search_op_log_events: %{has_more_events?: false}},
         :extend_previous
       ),
       do: :ok

  defp event_page_available(
         %{
           tailing?: tailing?,
           lql_rules: rules,
           search_op_log_events: %{has_more_events?: true}
         },
         :extend_previous
       ) do
    if unbounded_event_pagination?(tailing?, rules), do: :ok, else: :error
  end

  defp event_page_available(%{next_events_exhausted?: false}, :extend_next), do: :ok
  defp event_page_available(_assigns, _intent), do: :error

  defp bounded_event_pagination?(true, _rules), do: false

  defp bounded_event_pagination?(false, rules) do
    rules
    |> Rules.get_timestamp_filters()
    |> bounded_timestamp_filters?()
  end

  defp unbounded_event_pagination?(true, _rules), do: false

  defp unbounded_event_pagination?(false, rules),
    do: not bounded_event_pagination?(false, rules)

  defp bounded_timestamp_filters?(filters) do
    Enum.any?(filters, &(&1.operator == :range and length(&1.values || []) == 2)) or
      (Enum.any?(filters, &(&1.operator in [:>, :>=])) and
         Enum.any?(filters, &(&1.operator in [:<, :<=])))
  end

  defp put_event_page(socket, rows, :previous) do
    socket
    |> stream(:log_events, rows, at: -1)
    |> put_event_page_cursor(:previous, List.last(rows))
  end

  defp put_event_page(socket, rows, :next) do
    socket
    |> stream(:log_events, Enum.reverse(rows), at: 0)
    |> put_event_page_cursor(:next, List.first(rows))
  end

  @spec put_event_page_cursor(
          Phoenix.LiveView.Socket.t(),
          SearchOperation.event_page_direction(),
          Logflare.LogEvent.t() | nil
        ) :: Phoenix.LiveView.Socket.t()
  defp put_event_page_cursor(socket, _direction, nil), do: socket

  defp put_event_page_cursor(socket, direction, event) do
    cursors = Map.put(socket.assigns.event_page_cursors, direction, event_cursor(event))
    assign(socket, :event_page_cursors, cursors)
  end

  @spec event_cursor(Logflare.LogEvent.t() | nil) :: SearchOperation.event_cursor() | nil
  defp event_cursor(nil), do: nil

  defp event_cursor(%{id: id, body: body}) do
    %{id: id || body["id"], timestamp: body["timestamp"]}
  end

  defp put_search_events(socket, [])
       when socket.assigns.tailing? and not socket.assigns.tailing_initial?,
       do: socket

  defp put_search_events(socket, rows)
       when socket.assigns.tailing? and not socket.assigns.tailing_initial? do
    rows
    |> Enum.with_index()
    |> Enum.reduce(socket, fn {row, index}, socket ->
      stream_insert(socket, :log_events, row, at: index, limit: @log_event_stream_limit)
    end)
  end

  defp put_search_events(socket, rows) do
    socket
    |> stream(:log_events, rows, reset: true)
    |> assign(:event_page_cursors, %{
      previous: rows |> List.last() |> event_cursor(),
      next: rows |> List.first() |> event_cursor()
    })
  end

  defp event_search_metadata(events_op, has_more_events? \\ nil) do
    has_more_events? =
      if is_nil(has_more_events?), do: events_op.has_more_events?, else: has_more_events?

    %{events_op | rows: [], has_more_events?: has_more_events?}
  end

  defp apply_event_page_result(socket, events_op, direction) do
    has_more_events? =
      case direction do
        :previous -> events_op.has_more_events?
        :next -> socket.assigns.search_op_log_events.has_more_events?
      end

    events_metadata = event_search_metadata(events_op, has_more_events?)

    socket
    |> put_event_page(events_op.rows, direction)
    |> assign(:search_op, events_metadata)
    |> assign(:search_op_error, nil)
    |> assign(:search_op_log_events, events_metadata)
    |> assign(:event_page_loading, nil)
    |> assign(:last_query_completed_at, DateTime.utc_now())
  end

  defp apply_range_extension_result(socket, events_op) do
    %{request: request, cursor: cursor, has_more?: has_more?} = events_op.event_page_result
    direction = SearchOperation.event_page_direction(request.intent)
    has_more_events? = socket.assigns.search_op_log_events.has_more_events?
    events_metadata = event_search_metadata(events_op, has_more_events?)

    socket =
      socket
      |> assign(:search_op, events_metadata)
      |> assign(:search_op_error, nil)
      |> assign(:search_op_log_events, events_metadata)
      |> assign(:last_query_completed_at, DateTime.utc_now())
      |> put_extension_state(direction, not has_more?)

    if is_nil(cursor) do
      socket
      |> put_event_page(events_op.rows, direction)
      |> assign(:event_page_loading, nil)
    else
      lql_rules =
        socket.assigns.lql_rules
        |> Rules.extend_timestamp_range(
          direction,
          cursor.timestamp,
          socket.assigns.search_timezone
        )
        |> maybe_adjust_chart_period()

      querystring = Lql.encode!(lql_rules)

      socket =
        socket
        |> assign(:lql_rules, lql_rules)
        |> assign(:querystring, querystring)
        |> assign(:chart_loading, true)
        |> assign(:range_extension_patch, %{
          querystring: querystring,
          rows: events_op.rows,
          direction: direction
        })

      SearchQueryExecutor.query_agg(socket.assigns.executor_pid, socket.assigns)

      push_patch_with_params(socket, %{querystring: querystring, tailing?: false})
    end
  end

  defp put_extension_state(socket, :previous, _exhausted?), do: socket

  defp put_extension_state(socket, :next, exhausted?),
    do: assign(socket, :next_events_exhausted?, exhausted?)

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

  def handle_info(
        {:search_result,
         %{events: %{event_page_result: %{request: %{intent: :within_range}}} = events_op}},
        %{assigns: %{event_page_loading: :within_range}} = socket
      ) do
    {:noreply, apply_event_page_result(socket, events_op, :previous)}
  end

  def handle_info(
        {:search_result,
         %{
           events:
             %{
               event_page_result: %{
                 request: %{intent: intent, boundary: nil}
               }
             } = events_op
         }},
        %{assigns: %{event_page_loading: intent}} = socket
      )
      when intent in [:extend_previous, :extend_next] do
    direction = SearchOperation.event_page_direction(intent)
    {:noreply, apply_event_page_result(socket, events_op, direction)}
  end

  def handle_info(
        {:search_result,
         %{events: %{event_page_result: %{request: %{intent: intent}}} = events_op}},
        %{assigns: %{event_page_loading: intent}} = socket
      )
      when intent in [:extend_previous, :extend_next] do
    {:noreply, apply_range_extension_result(socket, events_op)}
  end

  def handle_info(
        {:search_result, %{events: %{event_page_result: %{}}}},
        socket
      ),
      do: {:noreply, socket}

  def handle_info(
        {:search_result, %{events: %{event_page_result: nil} = events_op} = search_result},
        socket
      ) do
    tailing_timer =
      if socket.assigns.tailing? do
        Process.send_after(self(), :schedule_tail_search, @tail_search_interval)
      end

    events_metadata = event_search_metadata(events_op)

    socket =
      socket
      |> reset_event_pagination()
      |> put_search_events(events_op.rows)
      |> assign(:search_op, events_metadata)
      |> assign(:search_op_error, nil)
      |> assign(:search_op_log_events, events_metadata)
      |> assign(:tailing_timer, tailing_timer)
      |> assign(:loading, false)
      |> assign(:tailing_initial?, false)
      |> assign(:last_query_completed_at, DateTime.utc_now())

    socket =
      if match?({:warning, _}, search_result.events.status) do
        {:warning, message} = search_result.events.status
        put_flash(socket, :info, message)
      else
        socket
      end

    {:noreply, socket}
  end

  def handle_info(
        {:search_error, %{event_page_request: %{intent: intent}}},
        %{assigns: %{event_page_loading: active_intent}} = socket
      )
      when intent != active_intent,
      do: {:noreply, socket}

  def handle_info({:search_error, search_op}, socket) do
    socket =
      case search_op.error do
        :halted ->
          send(self(), :soft_pause)

          socket
          |> assign(loading: false)
          |> assign(chart_loading: false)
          |> assign(event_page_loading: nil)
          |> put_halt_flash_message(search_op)

        err ->
          send(self(), :soft_pause)

          socket
          |> assign(loading: false)
          |> assign(chart_loading: false)
          |> assign(event_page_loading: nil)
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
    case Timex.Timezone.get(search_timezone) do
      {:error, _} -> timestamp_rules
      tz -> do_adjust_timestamp_rules(timestamp_rules, tz)
    end
  end

  defp do_adjust_timestamp_rules(timestamp_rules, tz) do
    Enum.map(timestamp_rules, fn lql_rule ->
      if Rules.timestamp_filter_rule_is_shorthand?(lql_rule) do
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
    |> assign(:event_page_loading, nil)
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
    |> reset_event_pagination()
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

  defp put_flash_query_error(socket, response) do
    message =
      case response do
        %QueryError{} = error -> QueryErrorHelpers.query_error_message(error)
        _ -> QueryErrorHelpers.generic_query_error_message()
      end

    put_flash(socket, :error, "Query halted: " <> message)
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
