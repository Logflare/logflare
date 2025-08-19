defmodule LogflareWeb.SearchLive.EventContextComponent do
  use LogflareWeb, :live_component

  alias Logflare.JSON
  alias Phoenix.LiveView.{AsyncResult, JS}
  alias Logflare.{Lql, SourceSchemas, Sources}
  alias Logflare.Source.BigQuery.SchemaBuilder
  import LogflareWeb.SearchLive.LogEventComponents, only: [log_event: 1]

  @impl true
  def update(assigns, socket) do
    %{
      params: %{
        "log-event-timestamp" => log_timestamp,
        "log-event-id" => log_event_id,
        "querystring" => query_string,
        "source-id" => source_id,
        "timezone" => timezone
      }
    } = assigns

    source_schema = SourceSchemas.Cache.get_source_schema_by(source_id: source_id)
    event_timestamp = log_timestamp |> String.to_integer() |> Timex.from_unix(:microsecond)

    timestamp_range =
      Lql.Rules.FilterRule.build(
        path: "timestamp",
        operator: :range,
        values: [
          Timex.shift(event_timestamp, days: -1) |> DateTime.truncate(:second),
          Timex.shift(event_timestamp, days: 1) |> DateTime.truncate(:second)
        ]
      )

    lql_rules =
      with {:ok, lql_rules} <-
             Lql.decode(
               query_string,
               Map.get(
                 source_schema || %{},
                 :bigquery_schema,
                 SchemaBuilder.initial_table_schema()
               )
             ) do
        Lql.Rules.update_timestamp_rules(lql_rules, [timestamp_range])
      end

    {:ok,
     socket
     |> assign(lql_rules: lql_rules)
     |> assign(target_event_id: log_event_id, timezone: timezone)
     |> assign(is_truncated_before: false)
     |> assign(is_truncated_after: false)
     |> assign(:logs, AsyncResult.loading())
     |> start_async(:logs, fn ->
       search_logs(log_event_id, event_timestamp, source_id, lql_rules)
     end)}
  end

  def handle_async(
        :logs,
        {:ok,
         %{
           events: events,
           is_truncated_before: before_truncated,
           is_truncated_after: after_truncated
         }},
        socket
      ) do
    {:noreply,
     socket
     |> assign(:logs, AsyncResult.ok(socket.assigns.logs, :ok))
     |> assign(:is_truncated_before, before_truncated)
     |> assign(:is_truncated_after, after_truncated)
     |> stream(:log_events, events, reset: true)}
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
        <div class="tw-font-mono tw-text-white tw-text-sm tw-space-x-2">
          <.lql_rules rules={@lql_rules} />
        </div>
      </div>
      <div class="tw-h-[calc(100vh-200px)] tw-overflow-y-auto tw-pr-2 tw-pl-5 -tw-ml-5" id="context_log_events" phx-hook="ScrollIntoView" phx-value-scroll-target={@target_event_id}>
        <.async_result assign={@logs}>
          <:loading><%= live_react_component("Components.Loader", %{}, id: "shared-loader") %></:loading>

          <div :if={@is_truncated_before} class="tw-text-center tw-py-2 tw-uppercase tw-text-sm">
            Limit 50 events before selection
          </div>
          <ul class="list-unstyled console-text" id="log-events" phx-update="stream">
            <.log_event
              :for={{id, log_event} <- @streams.log_events}
              log_event={log_event}
              timezone={@timezone}
              id={id}
              class={[
                "tw-group tw-flex tw-flex-wrap tw-items-center tw-pr-1 tw-relative",
                if(highlight?(log_event, @target_event_id),
                  do: "tw-bg-gray-600 tw-outline-gray-500 tw-outline tw-my-2",
                  else: ""
                )
              ]}
            >
              <span :if={highlight?(log_event, @target_event_id)} phx-mounted={JS.dispatch("scrollIntoView")} class="tw-absolute -tw-left-4">
                <i class="fas fa-chevron-right"></i>
              </span>

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
          <div :if={@is_truncated_after} class="tw-text-center tw-pt-2 tw-uppercase tw-text-sm">
            Limit 50 events after selection
          </div>
        </.async_result>
      </div>
    </div>
    """
  end

  def highlight?(log_event, target_event_id) do
    log_event.id == target_event_id
  end

  attr :rules, :list

  def lql_rules(assigns) do
    rules = Lql.Rules.get_timestamp_filters(assigns.rules)

    assigns =
      assigns
      |> assign(:query, Lql.encode!(rules))

    ~H"""
    <%= @query %>
    """
  end

  attr :rule, Lql.Rules.FilterRule, required: true

  def lql_rule(%{rule: %{path: "timestamp", operator: :range}} = assigns) do
    ~H"""
    t:
    """
  end

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

  def search_logs(log_event_id, ts, source_id, lql_rules) do
    import Ecto.Query
    source = Sources.get_source_for_lv_param(source_id)
    partition_type = Sources.get_table_partition_type(source)

    so =
      %{
        lql_rules: lql_rules,
        timestamp: ts,
        source: source,
        tailing?: false
      }
      |> Logflare.Logs.SearchOperation.new()
      |> Logflare.Logs.Search.get_and_put_partition_by()

    %{values: [min, max]} =
      lql_rules
      |> Lql.Rules.get_timestamp_filters()
      |> Enum.find(fn rule -> rule.operator == :range end)

    before_query =
      from(so.source.bq_table_id)
      |> Logflare.Logs.LogEvents.partition_query([min, max], partition_type)
      |> where([t], t.timestamp < ^ts or (t.timestamp == ^ts and t.id <= ^log_event_id))
      |> order_by([t], desc: t.timestamp, desc: t.id)
      |> Ecto.Query.select([t], %{
        id: t.id,
        timestamp: t.timestamp,
        event_message: t.event_message,
        rank:
          fragment("1 - ROW_NUMBER() OVER (ORDER BY ? DESC, ? DESC)", t.timestamp, t.id)
          |> selected_as(:rank)
      })
      |> limit(51)
      |> subquery()
      |> Ecto.Query.select([t], t)

    after_query =
      from(so.source.bq_table_id)
      |> Logflare.Logs.LogEvents.partition_query([min, max], partition_type)
      |> where([t], t.timestamp > ^ts or (t.timestamp == ^ts and t.id > ^log_event_id))
      |> order_by([t], asc: t.timestamp, asc: t.id)
      |> Ecto.Query.select([t], %{
        id: t.id,
        timestamp: t.timestamp,
        event_message: t.event_message,
        rank:
          over(fragment("ROW_NUMBER()"), order_by: [asc: t.timestamp, asc: t.id])
          |> selected_as(:rank)
      })
      |> limit(50)
      |> subquery()
      |> Ecto.Query.select([t], t)

    # Combine and reorder
    query =
      before_query
      |> union_all(^after_query)
      |> subquery()
      |> Ecto.Query.select([t], %{
        id: t.id,
        timestamp: t.timestamp,
        event_message: t.event_message,
        rank: t.rank
      })
      |> order_by([t], asc: t.timestamp, asc: t.id)

    result = %{so | query: query} |> Logflare.Logs.SearchOperations.do_query()

    before_truncated = result.rows |> List.first() |> Map.get("rank") == -50
    after_truncated = result.rows |> List.last() |> Map.get("rank") == 50

    events =
      result.rows
      |> Enum.map(fn row ->
        row
        |> Map.drop([:rank])
        |> Logflare.LogEvent.make_from_db(%{source: source})
      end)

    %{events: events, is_truncated_before: before_truncated, is_truncated_after: after_truncated}
  end
end
