defmodule LogflareWeb.SearchLive.LogEventComponents do
  use LogflareWeb, :html
  use LogflareWeb, :routes

  use Phoenix.Component

  import LogflareWeb.Helpers.BqSchema
  import LogflareWeb.ModalLiveHelpers

  alias Logflare.DateTimeUtils
  alias Logflare.Lql
  alias Logflare.Sources.Source
  alias Phoenix.LiveView.JS

  @log_levels ~W(debug info warning error alert critical notice emergency)

  attr :search_op_log_events, :any, required: false, default: nil
  attr :last_query_completed_at, :any, required: false
  attr :loading, :boolean, default: false
  attr :search_timezone, :string, required: true
  attr :source, :map, required: true
  attr :tailing?, :boolean, default: false
  attr :querystring, :string, required: true
  attr :lql_rules, :list, required: true

  def logs_list(assigns) do
    ~H"""
    <div id="source-logs-search-list" data-last-query-completed-at={@last_query_completed_at} phx-hook="SourceLogsSearchList" class="mt-4">
      <%= if @loading do %>
        <div id="logs-list-loading" class="blurred list-unstyled console-text-list"></div>
      <% else %>
        <ul :if={@search_op_log_events} id="logs-list" class="list-unstyled console-text-list">
          <.log_event :for={log <- @search_op_log_events.rows} timezone={@search_timezone} log_event={log}>
            <%= log.body["event_message"] %>
            <:actions>
              <.logs_list_actions log={log} recommended_query={lql_with_recommended_fields(@lql_rules, log, @source)} source={@source} tailing?={@tailing?} search_timezone={@search_timezone} querystring={@querystring} />
            </:actions>
          </.log_event>
        </ul>
      <% end %>
    </div>
    """
  end

  attr :log, :any, required: true
  attr :recommended_query, :any, required: true
  attr :tailing?, :boolean, default: false
  attr :source, :any, required: true
  attr :search_timezone, :string, required: true
  attr :querystring, :string, required: true

  def logs_list_actions(assigns) do
    ~H"""
    <%= live_modal_show_link(
        component: LogflareWeb.Search.LogEventViewerComponent,
        class: "tw-text-[0.65rem]",
        modal_id: :log_event_viewer,
        title: "Log Event",
        phx_value_log_event_id: @log.id,
        phx_value_log_event_timestamp: @log.body["timestamp"],
        phx_value_lql: @recommended_query
      ) do %>
      <span>view</span>
    <% end %>
    <%= live_modal_show_link(
        component: LogflareWeb.SearchLive.EventContextComponent,
        click: JS.push("soft_pause"),
        close:
          if(@tailing?,
            do:
              JS.push("soft_play", target: "#source-logs-search-control")
              |> JS.push("close"),
            else: nil
          ),
        class: "tw-text-[0.65rem]",
        modal_id: :log_event_context_viewer,
        title: "View Event Context",
        phx_value_log_event_id: @log.id,
        phx_value_source_id: @source.id,
        phx_value_log_event_timestamp: @log.body["timestamp"],
        phx_value_timezone: @search_timezone,
        phx_value_querystring: @querystring
      ) do %>
      <span>context</span>
    <% end %>

    <.link
      class="tw-text-[0.65rem] group-hover:tw-visible tw-invisible"
      phx-click={
        JS.dispatch("logflare:copy-to-clipboard",
          detail: %{
            text: "#{formatted_timestamp(@log, @search_timezone)}    #{@log.body["event_message"]}"
          }
        )
      }
      data-toggle="tooltip"
      data-placement="top"
      title="Copy to clipboard"
    >
      copy
    </.link>
    <.log_event_permalink log_event_id={@log.id} timestamp={@log.body["timestamp"]} source={@source} lql={@recommended_query} class="tw-text-[0.65rem] group-hover:tw-visible tw-invisible" />
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

    case Jason.decode(event.body["event_message"]) do
      {:ok, event_message} ->
        new_filter_rules =
          fields
          |> Enum.reject(&MapSet.member?(existing_filter_paths, &1))
          |> Enum.filter(&Map.has_key?(event_message, strip_meta(&1)))
          |> Enum.map(fn field_name ->
            Lql.Rules.FilterRule.build(
              path: field_name,
              operator: :=,
              value: Map.get(event_message, strip_meta(field_name))
            )
          end)

        (new_filter_rules ++ lql_rules)
        |> Lql.encode!()

      _ ->
        lql_rules
        |> Lql.encode!()
    end
  end

  defp strip_meta("metadata." <> k), do: k
  defp strip_meta(k), do: k

  attr :log_event, Logflare.LogEvent, required: true
  attr :id, :string, required: false
  attr :timezone, :string, required: true
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
end
