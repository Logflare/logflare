defmodule LogflareWeb.SearchLive.EventContextComponent do
  use LogflareWeb, :live_component

  alias Logflare.JSON
  alias Phoenix.LiveView.AsyncResult
  import LogflareWeb.SearchLive.LogEventComponents, only: [log_event: 1]
  import LogflareWeb.ModalLiveHelpers

  @impl true
  def update(assigns, socket) do
    %{
      params: %{
        "log-event-timestamp" => log_timestamp,
        "log-event-id" => log_event_id,
        "lql_rules" => lql_rules,
        "source_id" => source_id,
        "timezone" => timezone
      }
    } = assigns

    log_timestamp = String.to_integer(log_timestamp)

    {:ok,
     socket
     |> assign(lql_rules: lql_rules)
     |> assign(target_event_id: log_event_id, timezone: timezone)
     |> assign(:logs, AsyncResult.loading())
     |> start_async(:logs, fn ->
       search_logs(log_event_id, log_timestamp, source_id, lql_rules)
     end)}
  end

  def handle_async(:logs, {:ok, log_events}, socket) do
    {:noreply,
     socket
     |> assign(:logs, AsyncResult.ok(socket.assigns.logs, :ok))
     |> stream(:log_events, log_events, reset: true)}
  end

  def handle_async(:logs, {:exit, reason}, socket) do
    {:noreply,
     socket
     |> assign(:logs, AsyncResult.failed(socket.assigns.logs, reason))
     |> stream(:log_events, [], reset: true)}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div class="list-unstyled console-text-list -tw-mx-6 tw-relative">
      <div class="tw-flex tw-px-2 tw-py-4 tw-mb-4 tw-bg-gray-800 tw-items-baseline tw-sticky tw-w-full">
        <div class="tw-mr-3 tw-w-[9rem] tw-text-right">Query</div>
        <div class="tw-font-mono tw-text-white tw-text-sm tw-space-x-2">
          <.lql_rule :for={rule <- @lql_rules} :if={is_struct(rule, Logflare.Lql.Rules.FilterRule) && rule.path != "timestamp"} rule={rule} />
        </div>
      </div>
      <div class="tw-h-[calc(100vh-200px)] tw-overflow-y-auto tw-pr-2">
        <.async_result assign={@logs}>
          <:loading><%= live_react_component("Components.Loader", %{}, id: "shared-loader") %></:loading>

          <ul class="list-unstyled console-text" id="log-events" phx-update="stream">
            <.log_event
              :for={{_id, log_event} <- @streams.log_events}
              log_event={log_event}
              timezone={@timezone}
              class={[
                "tw-group tw-flex tw-flex-wrap tw-items-center tw-pr-1",
                if(highlight?(log_event, @target_event_id),
                  do: "tw-bg-gray-600 tw-outline-gray-500 tw-outline tw-my-2",
                  else: ""
                )
              ]}
            >
              <span class="tw-truncate tw-flex-1 tw-pr-1">
                <%= log_event.body["event_message"] %>
              </span>
              <:actions>
                <a class="metadata-link " data-toggle="collapse" href={"#metadata-" <> log_event.id} aria-expanded="false" class="tw-text-[0.65rem]">
                  event body
                </a>
                <div class="collapse metadata tw-overflow-hidden" id={"metadata-" <> log_event.id}>
                  <pre class="pre-metadata text-clip tw-overflow-x-auto"><code class="tw-text-nowrap"><%= JSON.encode!(log_event.body, pretty: true) %></code></pre>
                </div>
              </:actions>
            </.log_event>
          </ul>
        </.async_result>
      </div>
    </div>
    """
  end

  def highlight?(log_event, target_event_id) do
    log_event.id == target_event_id
  end

  attr :rule, Logflare.Lql.Rules.FilterRule, required: true

  def lql_rule(assigns) do
    operator =
      case assigns.rule.operator do
        := -> ":"
        other -> other |> to_string()
      end

    assigns =
      assigns
      |> assign(:operator, operator)

    ~H"""
    <span><%= @rule.path %><%= @operator %><%= @rule.value %></span>
    """
  end

  def search_logs(log_event_id, timestamp, source_id, lql_rules) do
    import Ecto.Query
    source = Logflare.Sources.get_source_for_lv_param(source_id)

    so =
      %{
        lql_rules: lql_rules,
        timestamp: timestamp,
        source: source,
        tailing?: false
      }
      |> Logflare.Logs.SearchOperation.new()
      |> Logflare.Logs.Search.get_and_put_partition_by()

    # target_event =
    #   from(t in so.source.bq_table_id)
    #   |> where([t], t.d == ^log_event_id)
    #   |> where([t], t.timestamp > ^timestamp)
    #   |> Ecto.Query.select([t], %{"target_timestamp" => t.timestamp})
    #   |> limit(1)

    # SELECT timestamp as target_ts
    #   FROM `logflare-dev-464423.1_dev.9db56741_41ca_4fe8_8c05_051a76a4c5d6`
    #   WHERE id = "4984baad-b0bc-4247-9800-4388e6d95f76"
    #     AND timestamp > TIMESTAMP("2025-08-01")
    #   LIMIT 1
    # ),
    #

    min = Timex.from_unix(timestamp, :microsecond)

    rows_after_query =
      from(so.source.bq_table_id)
      |> Ecto.Query.select([t], [t.timestamp, t.id, t.event_message])
      |> where(
        [t],
        fragment("EXTRACT(DATE FROM ?)", t.timestamp) >= ^Timex.to_date(min) and
          t.timestamp >= ^min
      )
      |> order_by([t], asc: t.timestamp)
      |> limit(50)

    rows_before_query =
      from(so.source.bq_table_id)
      |> Ecto.Query.select([t], [t.timestamp, t.id, t.event_message])
      |> where(
        [t],
        fragment("EXTRACT(DATE FROM ?)", t.timestamp) <= ^Timex.to_date(min) and
          t.timestamp < ^min
      )
      |> order_by([t], desc: t.timestamp)
      |> limit(50)

    tasks = [
      Task.async(fn ->
        %{so | query: rows_before_query} |> Logflare.Logs.SearchOperations.do_query()
      end),
      Task.async(fn ->
        %{so | query: rows_after_query} |> Logflare.Logs.SearchOperations.do_query()
      end)
    ]

    [search_op_before, search_op_after] = Task.await_many(tasks)

    search_op_before.rows |> dbg

    (search_op_before.rows ++ search_op_after.rows)
    |> Enum.map(&Logflare.LogEvent.make_from_db(&1, %{source: source}))
  end
end
