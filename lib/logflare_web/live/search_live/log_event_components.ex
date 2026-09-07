defmodule LogflareWeb.SearchLive.LogEventComponents do
  use LogflareWeb, :html
  use LogflareWeb, :routes

  use Phoenix.Component

  import LogflareWeb.Helpers.BqSchema
  import LogflareWeb.ModalLiveHelpers

  alias Logflare.DateTimeUtils
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.Logs.SearchOperation
  alias Logflare.Lql
  alias Logflare.Lql.Rules
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Sources.Source
  alias LogflareWeb.FormattedTimestampComponent
  alias LogflareWeb.Search.LogEventViewerComponent
  alias Phoenix.LiveView.JS

  @log_levels ~W(debug info warning error alert critical notice emergency)
  @default_empty_event_message "(empty event message)"

  attr :search_op_log_events, SearchOperation, default: nil
  attr :search_op_log_aggregates, :map, default: nil
  attr :log_events, :any, default: []
  attr :loading, :boolean, required: true
  attr :tailing?, :boolean, default: false

  attr :pagination_buttons, :map,
    default: %{
      previous: %{state: :hidden, cursor: nil},
      next: %{state: :hidden, cursor: nil}
    }

  attr :search_timezone, :string, required: true
  attr :source_schema_flat_map, :map, default: %{}

  def results_list(assigns) do
    lql_rules =
      if assigns.search_op_log_events, do: assigns.search_op_log_events.lql_rules, else: []

    assigns =
      assigns
      |> assign(:select_fields, build_select_fields(lql_rules))
      |> assign(:earlier_result_dt, first_date_with_results(assigns.search_op_log_aggregates))

    ~H"""
    <div :if={@search_op_log_events} id="source-logs-search-list" data-tailing={if(@tailing?, do: "true", else: "false")} phx-hook="SourceLogsSearchList" class="mt-4 tw-relative">
      <.load_more_button id="load-more-events-top" intent="previous" state={@pagination_buttons.previous.state} cursor={@pagination_buttons.previous.cursor} />
      <ul id="logs-list" phx-update="stream" class={["list-unstyled console-text-list", if(@loading, do: "blurred", else: nil)]}>
        <.empty_result_list search_op_log_events={@search_op_log_events} earlier_result_dt={@earlier_result_dt} loading={@loading} />
        <.log_event :for={{dom_id, log} <- @log_events} id={dom_id} data-event-id={event_id(log)} data-event-timestamp={log.body["timestamp"]} timezone={@search_timezone} log_event={log} select_fields={@select_fields} source_schema_flat_map={@source_schema_flat_map}>
          {log.body["event_message"]}
          <:actions phx-no-format>
          <div class="group-has-[.log-event-selected-field]:tw-ml-[13rem] group-has-[.log-event-selected-field]:tw-pb-1.5 tw-inline-block">
            <.modal_link
                   component={LogEventViewerComponent}
                   click={JS.push("open_log_event_modal")}
                   close={JS.push("close_log_event_modal", target: "#source-logs-search-control") |> JS.push("close")}
                   class="tw-text-[0.65rem]"
                   modal_id={:log_event_viewer}
                   title="Log Event"
                   phx-value-log-event-id={log.id}
                   phx-value-log-event-timestamp={log.body["timestamp"]}
                 >
                   <span>view</span>
                 </.modal_link>
                 <.modal_link
                   component={LogflareWeb.SearchLive.EventContextComponent}
                   click={JS.push("open_log_event_modal")}
                   close={JS.push("close_log_event_modal", target: "#source-logs-search-control") |> JS.push("close")}
                   class="tw-text-[0.65rem]"
                   modal_id={:log_event_context_viewer}
                   title="View Event Context"
                   phx-value-log-event-id={log.id}
                   phx-value-log-event-timestamp={log.body["timestamp"]}
                 >
                   <span>context</span>
                 </.modal_link>

                 <.link
                   class="tw-text-[0.65rem] group-hover:tw-visible tw-invisible"
                   phx-click={
                     JS.dispatch("logflare:copy-to-clipboard",
                       detail: %{
                         text: formatted_for_clipboard(log, @search_op_log_events)
                       }
                     )
                   }
                   data-toggle="tooltip"
                   data-placement="top"
                   title="Copy to clipboard"
                 >copy</.link>
                <.log_event_permalink log_event_id={log.id} timestamp={log.body["timestamp"]} source={@search_op_log_events.source} lql={lql_with_recommended_fields(@search_op_log_events.lql_rules, log, @search_op_log_events.source)} class="tw-text-[0.65rem] group-hover:tw-visible tw-invisible" />
                </div>
               </:actions>
        </.log_event>
      </ul>
      <.load_more_button id="load-more-events-bottom" intent="next" state={@pagination_buttons.next.state} cursor={@pagination_buttons.next.cursor} />
    </div>
    """
  end

  attr :id, :string, required: true
  attr :intent, :string, values: ~w(previous next), required: true
  attr :state, :atom, values: ~w(hidden ready disabled)a, required: true
  attr :cursor, :map, default: nil

  def load_more_button(assigns) do
    ~H"""
    <div class={["tw-justify-center", if(@state == :hidden, do: "tw-hidden", else: "tw-flex")]}>
      <button id={@id} type="button" class="tw-group btn btn-outline-secondary btn-sm tw-text-xs" phx-click="load_events" phx-value-intent={@intent} phx-value-cursor-id={@cursor && @cursor.id} phx-value-cursor-timestamp={@cursor && @cursor.timestamp} disabled={@state != :ready}>
        <span class="tw-relative tw-whitespace-nowrap tw-w-20 tw-flex tw-justify-center">
          <i class="spinner-border spinner-border-sm text-info tw-mr-1 tw-hidden group-[.phx-click-loading]:tw-inline" aria-hidden="true"></i>
          <span class="group-[.phx-click-loading]:tw-hidden">Load more</span>
          <span class="tw-hidden group-[.phx-click-loading]:tw-inline">Loading</span>
        </span>
      </button>
    </div>
    """
  end

  @spec lql_with_recommended_fields(Lql.Rules.lql_rules(), Logflare.LogEvent.t(), Source.t()) ::
          String.t()
  def lql_with_recommended_fields(lql_rules, event, source) do
    fields =
      source
      |> Source.recommended_query_fields()
      |> Enum.map(&Source.query_field_name/1)
      |> Enum.uniq()

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
        FilterRule.build(
          path: field_name,
          operator: :=,
          value: Map.get(event.body, strip_meta(field_name))
        )
      end)

    (new_filter_rules ++ lql_rules)
    |> Lql.encode!()
  end

  defp strip_meta(field), do: field |> Source.query_field_name() |> strip_metadata_prefix()

  defp strip_metadata_prefix("metadata." <> key), do: key
  defp strip_metadata_prefix(key), do: key

  def formatted_for_clipboard(log, search_op) do
    select_fields =
      build_select_fields(search_op.lql_rules)
      |> Enum.map(fn %{display: display, key: key} ->
        value = get_field_value(log.body, key)
        separator = if String.length(value) > 64, do: "\n", else: " "
        [display, ":", separator, value, "\n"]
      end)
      |> Enum.join("\n")

    """
    #{format_timestamp_for_clipboard(log.body["timestamp"], search_op.search_timezone)}    #{log.body["event_message"]}

    #{select_fields}
    """
  end

  defp format_timestamp_for_clipboard(timestamp, timezone) do
    format_timestamp(timestamp, timezone) <> timezone_offset(timezone)
  end

  defp timezone_offset(timezone) when is_binary(timezone) do
    case Timex.Timezone.get(timezone) do
      {:error, _} -> DateTimeUtils.humanize_timezone_offset(0)
      timezone_info -> DateTimeUtils.humanize_timezone_offset(timezone_info.offset_utc)
    end
  end

  attr :log_event, Logflare.LogEvent, required: true
  attr :id, :string, required: false
  attr :timezone, :string, required: true
  attr :empty_event_message_placeholder, :string, default: @default_empty_event_message
  attr :select_fields, :list, default: []
  attr :source_schema_flat_map, :map, default: %{}
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
        <.selected_fields :if={@select_fields != []} log_event={@log_event} select_fields={@select_fields} source_schema_flat_map={@source_schema_flat_map} timezone={@timezone} />
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

  attr :log_event, Logflare.LogEvent, required: true
  attr :select_fields, :list, required: true
  attr :source_schema_flat_map, :map, default: %{}
  attr :timezone, :string, default: nil

  def selected_fields(assigns) do
    ~H"""
    <div id={["log-", @log_event.id, "-selected-fields"]} class="has-[.log-event-selected-field]:tw-block tw-hidden">
      <%= for field <- @select_fields do %>
        <div :if={not is_nil(@log_event.body[field.key])} class="tw-text-neutral-200 tw-flex tw-leading-6 log-event-selected-field">
          <div class="tw-w-[13rem] tw-text-right ">
            <span class="tw-whitespace-nowrap tw-w-fit tw-px-1 tw-py-0.5 tw-bg-neutral-600 tw-text-white tw-mr-2">{truncate_display(field.display)}</span>
          </div>
          <span class="tw-text-white">
            {get_field_value(@log_event.body, field.key)}
            <FormattedTimestampComponent.formatted_timestamp :if={datetime_field?(field.path, @source_schema_flat_map)} value={Map.get(@log_event.body, field.key)} timezone={@timezone} />
          </span>
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

  defp build_select_fields(lql_rules) when is_list(lql_rules) do
    lql_rules
    |> Lql.Rules.get_select_rules()
    |> Enum.map(fn
      %{path: path, alias: nil} ->
        key = String.replace(path, ".", "_")
        display = path |> String.split(".") |> List.last()
        %{display: display, key: key, path: path}

      %{path: path, alias: alias} ->
        %{display: alias, key: alias, path: path}
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

  attr :search_op_log_events, :map, default: nil
  attr :earlier_result_dt, :any, default: nil
  attr :loading, :boolean, default: false

  def empty_result_list(assigns) do
    ~H"""
    <li id="empty-search-results" class={["tw-mt-4 tw-px-4 tw-py-3 tw-text-center tw-font-sans only:tw-block hidden", if(@loading, do: "tw-hidden", else: nil)]}>
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
    </li>
    """
  end

  defp event_id(%Logflare.LogEvent{id: id, body: body}), do: id || body["id"]

  def extended_search_lql(datetime) do
    new_rule =
      FilterRule.build(
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

  @spec datetime_field?(String.t() | [term()], map() | nil) :: boolean()
  def datetime_field?(path, source_schema_flat_map)
      when is_binary(path) and is_map(source_schema_flat_map) do
    path
    |> String.split(".")
    |> datetime_field?(source_schema_flat_map)
  end

  def datetime_field?(path, source_schema_flat_map)
      when is_list(path) and is_map(source_schema_flat_map) do
    SchemaUtils.get_type_for_path(path, source_schema_flat_map) == :datetime
  end

  def datetime_field?(_path, _source_schema_flat_map), do: false

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
