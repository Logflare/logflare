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

  attr :log_event, Logflare.LogEvent, required: true
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

    ~H"""
    <li id={"log-event_#{@log_event.id || @log_event.body["timestamp"]}"} {@rest}>
      <.metadata timestamp={@timestamp} log_level={@log_level}>
        <%= format_timestamp(@timestamp, @timezone) %>
      </.metadata>
      <%= render_slot(@inner_block) || @message %>
      <%= render_slot(@actions) %>
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
      <%= render_slot(@inner_block) %>
    </mark>
    <mark :if={@log_level} class={"log-level-#{@log_level}"}><%= @log_level %></mark>
    """
  end

  def formatted_timestamp(log_event, timezone) do
    tz_part =
      Timex.Timezone.get(timezone).offset_utc
      |> DateTimeUtils.humanize_timezone_offset()

    format_timestamp(log_event.body["timestamp"], timezone) <> tz_part
  end
end
