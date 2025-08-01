defmodule LogflareWeb.SearchLive.EventPeekComponent do
  use LogflareWeb, :live_component

  alias Logflare.JSON

  @impl true
  def update(assigns, socket) do
    log =
      %{
        "timestamp" => "2025-07-28 06:54:26",
        "id" => "8429494b-0e1b-491d-a224-74ee8c265294",
        "event_message" =>
          "{\"name\":\"banana\",\"qty\":12,\"store\":{\"address\":\"123 W Main St\",\"city\":\"Phoenix\",\"state\":\"AZ\",\"zip\":85016},\"tags\":[\"popular, tropical, organic\"],\"type\":\"fruit\",\"yellow\":true}",
        "metadata" => [
          %{
            "name" => "banana",
            "qty" => "12",
            "store" => [
              %{
                "address" => "123 W Main St",
                "city" => "Phoenix",
                "state" => "AZ",
                "zip" => "85016"
              }
            ],
            "tags" => ["popular", "tropical", "organic"],
            "type" => "fruit",
            "yellow" => "true",
            "level" => nil
          }
        ]
      }

    logs =
      0..200
      |> Enum.reduce([], fn _, acc -> [%{body: log, via_rule: nil} | acc] end)

    {:ok, assign(socket, logs: logs)}
  end

  def render(assigns) do
    ~H"""
    <div class="list-unstyled console-text-list">
      <ul class="list-unstyled">
        <.log_event :for={log <- @logs} log={log} />
      </ul>
    </div>
    """
  end

  attr :log, :map, required: true

  def log_event(assigns) do
    ~H"""
    <li class="hover:tw-bg-gray-800">
      <div class="console-text flex flex-col">
        <mark class="log-datestamp tw-inline-flex" data-timestamp={@log.body["timestamp"]}>
          <%= @log.body["timestamp"] %>
        </mark>
        <code class="tw-text-nowrap flex-1 console-text"><%= @log.body["event_message"] %></code>
      </div>
      <a class="metadata-link" data-toggle="collapse" href="#metadata-inx" aria-expanded="false" class="tw-text-[0.65rem]">
        event body
      </a>
      <div class="collapse metadata" id="metadata-inx">
        <pre class="pre-metadata text-clip"><code class="tw-text-nowrap"><%= JSON.encode!(@log.body, pretty: true) %></code></pre>
      </div>
      <%= if @log.via_rule do %>
        <span data-toggle="tooltip" data-placement="top" title={"Matching #{ @log.via_rule.lql_string } routing from #{@log.origin_source_id }"} style="color: ##5eeb8f;">
          <i class="fa fa-code-branch" style="font-size: 1em;"></i>
        </span>
      <% end %>
    </li>
    """
  end
end
