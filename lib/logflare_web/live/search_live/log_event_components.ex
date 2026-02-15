defmodule LogflareWeb.SearchLive.LogEventComponents do
  use LogflareWeb, :html
  use LogflareWeb, :routes

  use Phoenix.Component

  import LogflareWeb.Helpers.BqSchema
  import LogflareWeb.ModalLiveHelpers

  alias Logflare.DateTimeUtils
  alias Logflare.Lql
  alias Logflare.Lql.Rules
  alias Logflare.Sources.Source
  alias Phoenix.LiveView.JS

  @log_levels ~W(debug info warning error alert critical notice emergency)
  @default_empty_event_message "(empty event message)"

  attr :search_op_log_events, :map, default: nil
  attr :last_query_completed_at, :any, default: nil
  attr :loading, :boolean, required: true
  attr :search_timezone, :string, required: true
  attr :tailing?, :boolean, required: true
  attr :querystring, :string, required: true
  attr :empty_event_message_placeholder, :string, default: @default_empty_event_message
  attr :search_op, Logflare.Logs.SearchOperation

  def results_list(assigns) do
    assigns = assign(assigns, :select_fields, build_select_fields(assigns.search_op))

    ~H"""
    <div :if={@search_op_log_events} id="source-logs-search-list" data-last-query-completed-at={@last_query_completed_at} phx-hook="SourceLogsSearchList" class="mt-4">
      <ul id="logs-list" class={["list-unstyled console-text-list", if(@loading, do: "blurred", else: nil)]}>
        <.log_event :for={log <- @search_op_log_events.rows} timezone={@search_timezone} log_event={log} select_fields={build_select_fields(@search_op)}>
          {log.body["event_message"]}
          <:actions phx-no-format>
          <div class={if(Enum.any?(@select_fields), do: "tw-ml-[13rem] tw-pb-1.5", else: "tw-inline")}>
          <.modal_link
                   component={LogflareWeb.Search.LogEventViewerComponent}
                   class="tw-text-[0.65rem]"
                   modal_id={:log_event_viewer}
                   title="Log Event"
                   phx-value-log-event-id={log.id}
                   phx-value-log-event-timestamp={log.body["timestamp"]}
                   phx-value-lql={@querystring}
                   phx-value-tailing?={@tailing?}
                 >
                   <span>view</span>
                 </.modal_link>
                 <.modal_link
                   component={LogflareWeb.SearchLive.EventContextComponent}
                   click={JS.push("soft_pause")}
                   close={if(@tailing?, do: JS.push("soft_play", target: "#source-logs-search-control") |> JS.push("close"), else: nil)}
                   class="tw-text-[0.65rem]"
                   modal_id={:log_event_context_viewer}
                   title="View Event Context"
                   phx-value-log-event-id={log.id}
                   phx-value-source-id={@search_op.source.id}
                   phx-value-log-event-timestamp={log.body["timestamp"]}
                   phx-value-timezone={@search_timezone}
                   phx-value-querystring={@querystring}
                 >
                   <span>context</span>
                 </.modal_link>

                 <.link
                   class="tw-text-[0.65rem] group-hover:tw-visible tw-invisible"
                   phx-click={
                     JS.dispatch("logflare:copy-to-clipboard",
                       detail: %{
                         text: formatted_for_clipboard(log, @search_op)
                       }
                     )
                   }
                   data-toggle="tooltip"
                   data-placement="top"
                   title="Copy to clipboard"
                 >copy</.link>
                <.log_event_permalink log_event_id={log.id} timestamp={log.body["timestamp"]} source={@search_op.source} lql={lql_with_recommended_fields(@search_op.lql_rules, log, @search_op.source)} class="tw-text-[0.65rem] group-hover:tw-visible tw-invisible" />
                </div>
               </:actions>
        </.log_event>
      </ul>
    </div>
    """
  end

  @spec lql_with_recommended_fields(Lql.Rules.lql_rules(), Logflare.LogEvent.t(), Source.t()) ::
          String.t()
  def lql_with_recommended_fields(lql_rules, event, source) do
    fields = Source.recommended_query_fields(source)

    existing_filter_paths =
      lql_rules
      |> Lql.Rules.get_filter_rules()
      |> Enum.map(& &1.path)
      |> MapSet.new()

    new_filter_rules =
      fields
      |> Enum.reject(&MapSet.member?(existing_filter_paths, &1))
      |> Enum.filter(&Map.has_key?(event.body, strip_meta(&1)))
      |> Enum.map(fn field_name ->
        Lql.Rules.FilterRule.build(
          path: field_name,
          operator: :=,
          value: Map.get(event.body, strip_meta(field_name))
        )
      end)

    (new_filter_rules ++ lql_rules)
    |> Lql.encode!()
  end

  defp strip_meta("metadata." <> k), do: k
  defp strip_meta(k), do: k

  def formatted_for_clipboard(log, search_op) do
    select_fields =
      search_op
      |> build_select_fields()
      |> Enum.map(fn %{display: display, key: key} ->
        value = get_field_value(log.body, key)
        separator = if String.length(value) > 64, do: "\n", else: " "
        [display, ":", separator, value, "\n"]
      end)
      |> Enum.join("\n")

    """
    #{LogflareWeb.SearchLive.LogEventComponents.formatted_timestamp(log, search_op.search_timezone)}    #{log.body["event_message"]}

    #{select_fields}
    """
  end

  attr :log_event, Logflare.LogEvent, required: true
  attr :id, :string, required: false
  attr :timezone, :string, required: true
  attr :empty_event_message_placeholder, :string, default: @default_empty_event_message
  attr :select_fields, :list, default: []
  attr :rest, :global, default: %{class: "tw-group"}
  slot :inner_block
  slot :actions

  def log_event(%{log_event: %{body: body}} = assigns) do
    message = body["event_message"]

    assigns =
      assigns
      |> assign(timestamp: body["timestamp"], message: message)
      |> assign(
        :log_level,
        if(body["level"] in @log_levels, do: body["level"], else: nil)
      )
      |> assign_new(:id, fn %{log_event: log_event} ->
        id = log_event.id || log_event.body["timestamp"]
        "log-event_" <> id
      end)

    ~H"""
    <li id={@id} {@rest}>
      <.metadata timestamp={@timestamp} log_level={@log_level}>
        {format_timestamp(@timestamp, @timezone)}
      </.metadata>
      <%= if @message do %>
        {render_slot(@inner_block) || @message}
        <.selected_fields :if={@select_fields != []} log_event={@log_event} select_fields={@select_fields} />
      <% else %>
        <span class="tw-italic tw-text-gray-500">{@empty_event_message_placeholder}</span>
      <% end %>
      {render_slot(@actions)}
    </li>
    """
  end

  attr :log_level, :string, default: nil
  attr :timestamp, :string
  slot :inner_block

  def metadata(assigns) do
    ~H"""
    <mark class={"log-#{@log_level} mr-2"} data-timestamp={@timestamp}>
      {render_slot(@inner_block)}
    </mark>
    <mark :if={@log_level} class={"log-level-#{@log_level}"}>{@log_level}</mark>
    """
  end

  def formatted_timestamp(log_event, timezone) do
    tz_part =
      Timex.Timezone.get(timezone).offset_utc
      |> DateTimeUtils.humanize_timezone_offset()

    format_timestamp(log_event.body["timestamp"], timezone) <> tz_part
  end

  attr :log_event, Logflare.LogEvent, required: true
  attr :select_fields, :list, required: true

  def selected_fields(assigns) do
    ~H"""
    <div id={["log-", @log_event.id, "-selected-fields"]}>
      <%= for field <- @select_fields do %>
        <div class="tw-text-neutral-200 tw-flex tw-leading-6">
          <div class="tw-w-[13rem] tw-text-right ">
            <span class="tw-whitespace-nowrap tw-w-fit tw-px-1 tw-py-0.5 tw-bg-neutral-600 tw-text-white tw-mr-2">{truncate_display(field.display)}</span>
          </div>
          <span class="tw-text-white">{get_field_value(@log_event.body, field.key)}</span>
        </div>
      <% end %>
    </div>
    """
  end

  defp get_field_value(body, field_key) when is_binary(field_key) do
    Map.get(body, field_key)
    |> format_field_value()
  end

  defp format_field_value(nil), do: "null"
  defp format_field_value(value) when is_binary(value), do: value
  defp format_field_value(value) when is_number(value), do: to_string(value)
  defp format_field_value(value) when is_boolean(value), do: to_string(value)
  defp format_field_value(value) when is_list(value), do: Jason.encode!(value)
  defp format_field_value(value) when is_map(value), do: Jason.encode!(value)
  defp format_field_value(value), do: inspect(value)

  defp build_select_fields(%{lql_rules: lql_rules}) do
    lql_rules
    |> Lql.Rules.get_select_rules()
    |> Enum.map(fn
      %{path: path, alias: nil} ->
        key = String.replace(path, ".", "_")
        display = path |> String.split(".") |> List.last()
        %{display: display, key: key}

      %{path: _path, alias: alias} ->
        %{display: alias, key: alias}
    end)
    |> Enum.reject(fn field -> is_nil(field.display) end)
  end

  defp build_select_fields(_), do: []

  defp truncate_display(display) when is_binary(display) do
    if String.length(display) > 23 do
      (String.slice(display, 0, 23) <> "&hellip;") |> raw()
    else
      display
    end
  end

  attr :search_op_log_aggregates, :map, default: nil
  attr :search_op_log_events, :map, default: nil

  def empty_result_list(assigns) do
    assigns =
      assigns
      |> assign(
        :earlier_result_dt,
        first_date_with_results(assigns.search_op_log_aggregates)
      )

    ~H"""
    <div :if={show_empty_results?(@search_op_log_events)} class="tw-mt-4 tw-px-4 tw-py-3 tw-text-center tw-font-sans">
      <h2 class="tw-text-lg tw-font-semibold tw-text-gray-400">
        No events matching your query
      </h2>
      <div :if={@earlier_result_dt}>
        <p>
          Hint: results were found in
          <span class="tw-text-sm tw-font-mono tw-select-all">
            {extended_search_lql(@earlier_result_dt) |> Lql.encode!()}
          </span>
        </p>
        <.link class="btn btn-primary tw-mt-2" patch={extended_search_url(@search_op_log_events, @earlier_result_dt)}>
          <i class="fas fa-search"></i><span class="fas-in-button hide-on-mobile">Extend search</span>
        </.link>
      </div>
    </div>
    """
  end

  defp show_empty_results?(%{rows: rows})
       when is_list(rows), do: Enum.empty?(rows)

  defp show_empty_results?(_), do: false

  def extended_search_lql(datetime) do
    new_rule =
      Lql.Rules.FilterRule.build(
        modifiers: %{},
        operator: :>=,
        path: "timestamp",
        shorthand: nil,
        value: DateTime.truncate(datetime, :second)
      )

    [new_rule]
  end

  def extended_search_url(search_op, datetime) do
    new_rule = extended_search_lql(datetime)

    query =
      search_op.lql_rules
      |> Rules.update_timestamp_rules(new_rule)
      |> Lql.encode!()

    ~p"/sources/#{search_op.source}/search?#{[querystring: query, tailing?: false]}"
  end

  defp first_date_with_results(%{rows: rows}) when is_list(rows) do
    Enum.find(
      rows,
      fn
        %{"value" => value} -> value > 0
        %{"total" => total} -> total > 0
        _ -> false
      end
    )
    |> case do
      %{"datetime" => datetime} -> datetime
      _ -> nil
    end
  end

  defp first_date_with_results(_), do: nil
end
