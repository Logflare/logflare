defmodule LogflareWeb.SearchLive.LogEventComponents do
  use LogflareWeb, :html
  use LogflareWeb, :routes

  use Phoenix.Component

  import LogflareWeb.Helpers.BqSchema
  import LogflareWeb.ModalLiveHelpers

  alias Logflare.DateTimeUtils
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.Lql
  alias Logflare.Lql.Rules
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Sources.Source
  alias LogflareWeb.FormattedTimestampComponent
  alias LogflareWeb.Search.LogEventViewerComponent
  alias Phoenix.LiveView.JS

  @log_levels ~W(debug info warning error alert critical notice emergency)
  @default_empty_event_message "(empty event message)"

  attr :search_op_log_events, :map, default: nil
  attr :last_query_completed_at, :any, default: nil
  attr :loading, :boolean, required: true
  attr :search_timezone, :string, required: true
  attr :empty_event_message_placeholder, :string, default: @default_empty_event_message
  attr :source_schema_flat_map, :map, default: %{}
  attr :search_op, Logflare.Logs.SearchOperation

  def results_list(assigns) do
    assigns = assign(assigns, :select_fields, build_select_fields(assigns.search_op))

    ~H"""
    <div :if={@search_op_log_events} id="source-logs-search-list" data-last-query-completed-at={@last_query_completed_at} phx-hook="SourceLogsSearchList" class="mt-4">
      <ul id="logs-list" class={["list-unstyled console-text-list", if(@loading, do: "blurred", else: nil)]}>
        <.log_event :for={log <- @search_op_log_events.rows} timezone={@search_timezone} log_event={log} select_fields={build_select_fields(@search_op)} source_schema_flat_map={@source_schema_flat_map}>
          {log.body["event_message"]}
          <:actions phx-no-format>
          <div class="group-has-[.log-event-selected-field]:tw-ml-[13rem] group-has-[.log-event-selected-field]:tw-pb-1.5 tw-inline-block">
            <.modal_link
                   component={LogEventViewerComponent}
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
                   click={JS.push("open_event_context")}
                   close={JS.push("close_event_context", target: "#source-logs-search-control") |> JS.push("close")}
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
      search_op
      |> build_select_fields()
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

  defp build_select_fields(%{lql_rules: lql_rules}) do
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
