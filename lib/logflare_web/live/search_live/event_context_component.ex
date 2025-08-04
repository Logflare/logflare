defmodule LogflareWeb.SearchLive.EventContextComponent do
  use LogflareWeb, :live_component

  alias Logflare.JSON
  use Timex

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
      0..100
      |> Enum.reduce([], fn _, acc ->
        ts =
          DateTime.utc_now()
          |> DateTime.add(:rand.uniform(1000), :second)
          |> Calendar.strftime("%Y-%m-%d %H:%M:%S")

        log = %{log | "timestamp" => ts}

        [
          %{id: Ecto.UUID.generate(), body: log, via_rule: nil, highlight?: length(acc) == 50}
          | acc
        ]
      end)

    {:ok, assign(socket, logs: logs)}
  end

  def render(assigns) do
    ~H"""
    <div class="list-unstyled console-text-list -tw-mx-6">
      <ul class="list-unstyled">
        <.log_event :for={log <- @logs} log={log} />
      </ul>
    </div>
    """
  end

  attr :log, :map, required: true

  def log_event(assigns) do
    ~H"""
    <li class={[
      "hover:tw-bg-gray-800",
      if(@log.highlight?, do: "tw-bg-gray-500", else: "")
    ]}>
      <div class="console-text tw-flex tw-flex-wrap tw-mb-0 tw-space-x-2">
        <mark class={["log-datestamp tw-grow-0", if(@log.highlight?, do: "tw-bg-gray-500 tw-text-white", else: "")]} data-timestamp={@log.body["timestamp"]}>
          <%= @log.body["timestamp"] %>
        </mark>
        <div class="tw-flex-1 tw-truncate">
          <code class="tw-text-nowrap  console-text"><%= @log.body["event_message"] %></code>
        </div>
        <a class={["metadata-link", if(@log.highlight?, do: "tw-bg-gray-500 tw-text-white", else: "")]} data-toggle="collapse" href={"#metadata-" <> @log.id} aria-expanded="false" class="tw-text-[0.65rem]">
          event body
        </a>
        <div class="tw-h-0 tw-basis-full"></div>
        <div class="collapse metadata tw-overflow-hidden" id={"metadata-" <> @log.id}>
          <pre class="pre-metadata text-clip tw-overflow-x-auto"><code class="tw-text-nowrap"><%= JSON.encode!(@log.body, pretty: true) %></code></pre>
        </div>
        <%= if @log.via_rule do %>
          <span data-toggle="tooltip" data-placement="top" title={"Matching #{ @log.via_rule.lql_string } routing from #{@log.origin_source_id }"} style="color: ##5eeb8f;">
            <i class="fa fa-code-branch" style="font-size: 1em;"></i>
          </span>
        <% end %>
      </div>
    </li>
    """
  end
end
