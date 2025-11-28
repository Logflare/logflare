defmodule LogflareWeb.SearchLive.LogEventComponents do
  use LogflareWeb, :html
  use LogflareWeb, :routes

  use Phoenix.Component

  import LogflareWeb.Helpers.BqSchema
  import LogflareWeb.ModalLiveHelpers, only: [modal_link: 1]

  alias Logflare.DateTimeUtils
  alias Logflare.Lql
  alias Phoenix.LiveView.JS

  @log_levels ~W(debug info warning error alert critical notice emergency)

  attr :search_op_log_events, :map, default: nil
  attr :last_query_completed_at, :any, default: nil
  attr :loading, :boolean, required: true
  attr :search_timezone, :string, required: true
  attr :tailing?, :boolean, required: true
  attr :querystring, :string, required: true
  attr :search_op, Logflare.Logs.SearchOperation

  def results_list(assigns) do
    assigns = assign(assigns, :select_fields, build_select_fields(assigns.search_op))

    ~H"""
    <div :if={@search_op_log_events} id="source-logs-search-list" data-last-query-completed-at={@last_query_completed_at} phx-hook="SourceLogsSearchList" class="mt-4">
      <ul id="logs-list" class={["list-unstyled console-text-list", if(@loading, do: "blurred", else: nil)]}>
        <.log_event :for={log <- @search_op_log_events.rows} timezone={@search_timezone} log_event={log} select_fields={build_select_fields(@search_op)}>
          {log.body["event_message"]}
          <:actions phx-no-format>
                <.modal_link
                  component={LogflareWeb.Search.LogEventViewerComponent}
                  class="tw-text-[0.65rem]"
                  modal_id={:log_event_viewer}
                  title="Log Event"
                  phx-value-log-event-id={log.id}
                  phx-value-log-event-timestamp={log.body["timestamp"]}
                  phx-value-lql={@querystring}
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
                        text: "#{LogflareWeb.SearchLive.LogEventComponents.formatted_timestamp(log, assigns[:search_timezone])}    #{log.body["event_message"]}"
                      }
                    )
                  }
                  data-toggle="tooltip"
                  data-placement="top"
                  title="Copy to clipboard"
                >copy</.link>
                <.log_event_permalink log_event_id={log.id} timestamp={log.body["timestamp"]} source={@search_op.source} lql={@querystring} class="tw-text-[0.65rem] group-hover:tw-visible tw-invisible" />
              </:actions>
        </.log_event>
      </ul>
    </div>
    <div :if={@search_op_log_events == nil}></div>
    """
  end

  attr :log_event, Logflare.LogEvent, required: true
  attr :id, :string, required: false
  attr :timezone, :string, required: true
  attr :select_fields, :list, default: []
  attr :rest, :global, default: %{class: "tw-group"}
  slot :inner_block
  slot :actions

  def log_event(%{log_event: %{body: body}} = assigns) when is_map_key(body, "event_message") do
    assigns =
      assigns
      |> assign(timestamp: body["timestamp"], message: body["event_message"])
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
      {render_slot(@inner_block) || @message}
      {render_slot(@actions)}
      <.selected_fields :if={@select_fields != []} log_event={@log_event} select_fields={@select_fields} />
    </li>
    """
  end

  def log_event(assigns), do: ~H""

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
    <div>
      <%= for field <- @select_fields do %>
        <div class="tw-text-neutral-200 tw-ml-52 last:tw-mb-2">
          <span class="">{field.display}:</span>
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
        %{display: path, key: key}

      %{path: path, alias: alias} ->
        %{display: alias, key: alias}
    end)
    |> Enum.reject(fn field -> is_nil(field.display) end)
  end
end
