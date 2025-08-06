defmodule LogflareWeb.SearchLive.LogEventComponents do
  use LogflareWeb, :html
  use LogflareWeb, :routes

  use Phoenix.Component

  import LogflareWeb.ModalLiveHelpers
  import LogflareWeb.CoreComponents
  import LogflareWeb.Helpers.BqSchema

  alias Logflare.DateTimeUtils
  alias Phoenix.LiveView.JS

  @log_levels ~W(debug info warning error alert critical notice emergency)

  attr :log, :map
  attr :search_timezone, :string
  attr :query_string, :string
  attr :source, :map

  def log_search_event(%{log: %{body: body}} = assigns) when is_map_key(body, "event_message") do
    tz_part =
      DateTimeUtils.humanize_timezone_offset(
        Timex.Timezone.get(assigns.search_timezone).offset_utc
      )

    assigns =
      assigns
      |> assign(timestamp: body["timestamp"], message: body["event_message"])
      |> assign(
        :formatted_timestamp,
        format_timestamp(body["timestamp"], assigns.search_timezone) <> tz_part
      )
      |> assign(
        :log_level,
        if(body["level"] in @log_levels, do: body["level"], else: nil)
      )

    ~H"""
    <li id={"log-event_#{@log.id || @log.body["timestamp"]}"} class="tw-group">
      <span class="tw-inline-block">
        <.metadata timestamp={@timestamp} log_level={@log_level}>
          <%= format_timestamp(@timestamp, @search_timezone) %>
        </.metadata>
        <%= @message %>
      </span>
      <span class="tw-inline-block tw-text-[0.65rem] tw-align-text-bottom tw-inline-flex tw-flex-row tw-gap-2">
        <%= live_modal_show_link(component: LogflareWeb.Search.LogEventViewerComponent, modal_id: :log_event_viewer, title: "Log Event", phx_value_log_event_id: @log.id, phx_value_log_event_timestamp: @log.body["timestamp"], phx_value_lql:  @query_string) do %>
          <span>view</span>
        <% end %>
        <.link class="tw-text-[0.65rem]  group-hover:tw-visible tw-invisible" phx-click={JS.dispatch("logflare:copy-to-clipboard", detail: %{text: "#{@formatted_timestamp}    #{@message}"})} data-toggle="tooltip" data-placement="top" title="Copy to clipboard">
          copy
        </.link>
        <.log_event_permalink log_event_id={@log.id} timestamp={@timestamp} source={@source} lql={@query_string} class="group-hover:tw-visible tw-invisible" />
      </span>
    </li>
    """
  end

  def log_search_event(assigns), do: ~H""

  attr :log_level, :string, default: nil
  attr :timestamp, :string
  slot :inner_block

  def metadata(assigns) do
    ~H"""
    <mark class={"log-#{@log_level} mr-2"} data-timestamp={@timestamp}>
      <%= render_slot(@inner_block) %>
    </mark>
    <mark :if={@log_level} class={"log-level-#{@log_level}"}><%= @log_level %></mark>
    """
  end
end
