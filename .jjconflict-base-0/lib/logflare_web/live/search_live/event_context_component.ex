defmodule LogflareWeb.SearchLive.EventContextComponent do
  use LogflareWeb, :live_component

  alias Logflare.JSON
  alias Phoenix.LiveView.{AsyncResult, JS}
  alias Logflare.{Lql, Logs, SourceSchemas, Sources.Source, Sources}
  alias Logflare.Sources.Source.BigQuery.SchemaBuilder
  import LogflareWeb.SearchLive.LogEventComponents, only: [log_event: 1]

  require Logger

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

    event_timestamp = log_timestamp |> String.to_integer() |> Timex.from_unix(:microsecond)

    lql_rules =
      Sources.get_source_for_lv_param(source_id)
      |> prepare_lql_rules(query_string, event_timestamp)

    {:ok,
     socket
     |> assign(lql_rules: lql_rules)
     |> assign(target_event_id: log_event_id, timezone: timezone)
     |> assign(is_truncated_before: false)
     |> assign(is_truncated_after: false)
     |> assign(source: Sources.get_source_for_lv_param(source_id))
     |> assign(:logs, AsyncResult.loading())
     |> start_async(:logs, fn ->
       search_logs(log_event_id, event_timestamp, source_id, lql_rules)
     end)}
  end

  @spec prepare_lql_rules(%Source{}, String.t(), DateTime.t()) :: Lql.Rules.lql_rules()
  def prepare_lql_rules(source, query_string, event_timestamp) do
    source_schema = SourceSchemas.Cache.get_source_schema_by(source_id: source.id)

    {:ok, lql_rules} =
      Lql.decode(
        query_string,
        Map.get(source_schema || %{}, :bigquery_schema, SchemaBuilder.initial_table_schema())
      )

    timestamp_range =
      Lql.Rules.FilterRule.build(
        path: "timestamp",
        operator: :range,
        values: [
          Timex.shift(event_timestamp, days: -1) |> DateTime.truncate(:second),
          Timex.shift(event_timestamp, days: 1) |> DateTime.truncate(:second)
        ]
      )

    required_fields =
      source
      |> Source.recommended_query_fields()
      |> Enum.map(&Source.query_field_name/1)

    lql_rules
    |> Logflare.Lql.Rules.get_filter_rules()
    |> Enum.filter(&(&1.path in required_fields))
    |> Lql.Rules.update_timestamp_rules([timestamp_range])
  end

  @impl true
  def handle_async(:logs, {:ok, %{rows: rows, source: source}}, socket) do
    before_truncated = rows |> List.first() |> Map.get("rank") == -50
    after_truncated = rows |> List.last() |> Map.get("rank") == 50

    events =
      rows
      |> Enum.map(fn row ->
        row
        |> Map.drop(["rank"])
        |> Logflare.LogEvent.make_from_db(%{source: source})
      end)

    {:noreply,
     socket
     |> assign(:logs, AsyncResult.ok(socket.assigns.logs, :ok))
     |> assign(:is_truncated_before, before_truncated)
     |> assign(:is_truncated_after, after_truncated)
     |> stream(:log_events, events, reset: true)}
  end

  def handle_async(:logs, {:ok, %{error: error, source: source}}, socket) do
    Logger.error("Backend context search error for source: #{source.token}",
      error_string: inspect(error),
      source_id: source.token
    )

    {:noreply,
     socket
     |> assign(:logs, AsyncResult.failed(socket.assigns.logs, "An error occurred."))
     |> stream(:log_events, [], reset: true)}
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
          {Lql.encode!(@lql_rules)}
        </div>
      </div>
      <div class="tw-h-[calc(100vh-200px)] tw-overflow-y-auto tw-pr-2 tw-pl-5 -tw-ml-5" id="context_log_events" phx-hook="ScrollIntoView" phx-value-scroll-target={@target_event_id}>
        <.async_result assign={@logs}>
          <:loading>{live_react_component("Components.Loader", %{}, id: "shared-loader")}</:loading>
          <:failed>
            <div id="context_log_events_error">An error occurred.</div>
          </:failed>

          <div :if={@is_truncated_before} class="tw-text-center tw-py-2 tw-uppercase tw-text-sm">
            Showing 50 events before selected event
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
                {log_event.body["event_message"]}
              </span>
              <:actions>
                <a class="metadata-link " data-toggle="collapse" href={"#metadata-" <> log_event.id} aria-expanded="false" class="tw-text-[0.65rem]">
                  event body
                </a>
                <div class="collapse metadata tw-overflow-hidden tw-basis-full" id={"metadata-" <> log_event.id}>
                  <pre class="pre-metadata text-clip tw-overflow-x-auto"><code class="tw-text-nowrap"><%= JSON.encode!(log_event.body, pretty: true) %></code></pre>
                </div>
              </:actions>
            </.log_event>
          </ul>
          <div :if={@is_truncated_after} class="tw-text-center tw-pt-2 tw-uppercase tw-text-sm">
            Showing 50 events after selected event
          </div>
        </.async_result>
      </div>
    </div>
    """
  end

  def highlight?(log_event, target_event_id) do
    log_event.id == target_event_id
  end

  @spec search_logs(String.t(), DateTime.t(), binary() | integer(), Lql.Rules.lql_rules()) ::
          map()
  def search_logs(log_event_id, ts, source_id, lql_rules) do
    source = Sources.get_source_for_lv_param(source_id)

    so =
      %{
        lql_rules: lql_rules,
        source: source,
        tailing?: false
      }
      |> Logs.SearchOperation.new()

    case Logs.Search.search_event_context(so, log_event_id, ts) do
      {:ok, %{rows: rows}} ->
        %{rows: rows, source: source}

      {:error, %{error: error}} ->
        %{error: error, source: source}
    end
  end
end
