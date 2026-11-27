defmodule LogflareWeb.SearchLive.EventContextComponent do
  use LogflareWeb, :live_component

  alias Logflare.JSON
  alias Logflare.Logs.EventPage
  alias Logflare.Lql.Rules, as: LqlRules
  alias Logflare.Lql.Rules.FilterRule
  alias Phoenix.LiveView.{AsyncResult, JS}
  alias Logflare.{Lql, Logs, SourceSchemas, Sources.Source, Sources}
  alias Logflare.Sources.Source.BigQuery.SchemaBuilder
  alias LogflareWeb.SearchLive.EventPagination
  import LogflareWeb.SearchLive.LogEventComponents, only: [log_event: 1, load_more_button: 1]

  @page_size Logs.Search.context_page_size()

  @impl true
  def update(assigns, socket) do
    %{
      params: %{
        "log-event-timestamp" => log_timestamp,
        "log-event-id" => log_event_id
      },
      querystring: query_string,
      search_timezone: timezone,
      source: source
    } = assigns

    context_key = {source.id, log_event_id, log_timestamp, query_string}
    socket = assign(socket, source: source, timezone: timezone)

    if socket.assigns[:context_key] == context_key do
      {:ok, socket}
    else
      source_id = source.id
      event_timestamp = log_timestamp |> String.to_integer() |> Timex.from_unix(:microsecond)
      lql_rules = prepare_lql_rules(source, query_string, event_timestamp)

      {:ok,
       socket
       |> assign(context_key: context_key, lql_rules: lql_rules, target_event_id: log_event_id)
       |> assign(is_truncated_before: false, is_truncated_after: false)
       |> assign(event_timestamp: event_timestamp, cursors: %{previous: nil, next: nil})
       |> assign(loading_direction: :initial, load_error: nil)
       |> assign(:logs, AsyncResult.loading())
       |> start_async(:logs, fn ->
         search_logs(log_event_id, event_timestamp, source_id, lql_rules)
       end)}
    end
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
      FilterRule.build(
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
    |> LqlRules.get_filter_rules()
    |> Enum.filter(&(&1.path in required_fields))
    |> Lql.Rules.update_timestamp_rules([timestamp_range])
  end

  @impl true
  def handle_async(:logs, {:ok, %{rows: rows, source: source}}, socket) do
    %{loading_direction: direction, cursors: cursors} = socket.assigns

    before_truncated =
      case direction do
        :initial -> match?(%{"rank" => rank} when rank < -@page_size, List.first(rows))
        :previous -> length(rows) > @page_size
        :next -> socket.assigns.is_truncated_before
      end

    after_truncated =
      case direction do
        :initial -> match?(%{"rank" => rank} when rank > @page_size, List.last(rows))
        :next -> length(rows) > @page_size
        :previous -> socket.assigns.is_truncated_after
      end

    rows =
      case direction do
        :initial -> Enum.filter(rows, &(&1["rank"] in -@page_size..@page_size))
        :previous -> Enum.take(rows, -@page_size)
        :next -> Enum.take(rows, @page_size)
      end

    events =
      rows
      |> Enum.map(fn row ->
        row
        |> Map.drop(["rank"])
        |> Logflare.LogEvent.make_from_db(%{source: source})
      end)

    cursors =
      case {direction, events} do
        {_, []} ->
          cursors

        {:initial, _} ->
          %{previous: event_cursor(hd(events)), next: event_cursor(List.last(events))}

        {:previous, _} ->
          %{cursors | previous: event_cursor(hd(events))}

        {:next, _} ->
          %{cursors | next: event_cursor(List.last(events))}
      end

    events = if direction == :previous, do: Enum.reverse(events), else: events

    {:noreply,
     socket
     |> assign(:logs, AsyncResult.ok(socket.assigns.logs, :ok))
     |> assign(:is_truncated_before, before_truncated)
     |> assign(:is_truncated_after, after_truncated)
     |> assign(cursors: cursors, loading_direction: nil)
     |> stream(:log_events, events,
       at: if(direction == :previous, do: 0, else: -1),
       reset: direction == :initial
     )}
  end

  def handle_async(:logs, {:ok, %{error: _error, source: _source}}, socket) do
    handle_async(:logs, {:exit, "An error occurred."}, socket)
  end

  def handle_async(:logs, {:exit, _reason}, %{assigns: %{logs: %{ok?: true}}} = socket) do
    {:noreply,
     assign(socket, loading_direction: nil, load_error: "Unable to load more events. Try again.")}
  end

  def handle_async(:logs, {:exit, reason}, socket) do
    {:noreply,
     socket
     |> assign(:logs, AsyncResult.failed(socket.assigns.logs, reason))
     |> stream(:log_events, [], reset: true)}
  end

  @impl true
  def handle_event("load_events", _params, %{assigns: %{loading_direction: direction}} = socket)
      when not is_nil(direction) do
    {:noreply, socket}
  end

  def handle_event("load_events", %{"intent" => intent}, socket)
      when intent in ["previous", "next"] do
    %{
      cursors: cursors,
      target_event_id: id,
      event_timestamp: timestamp,
      source: source,
      lql_rules: rules
    } = socket.assigns

    direction =
      case intent do
        "previous" -> :previous
        "next" -> :next
      end

    cursor = Map.fetch!(cursors, direction)

    if EventPage.valid_cursor?(cursor) do
      request = %{intent: direction, cursor: cursor}

      {:noreply,
       socket
       |> assign(loading_direction: direction, load_error: nil)
       |> start_async(:logs, fn -> search_logs(id, timestamp, source.id, rules, request) end)}
    else
      {:noreply, socket}
    end
  end

  @spec event_cursor(Logflare.LogEvent.t()) :: EventPage.cursor()
  defp event_cursor(event), do: %{id: event.id, timestamp: event.body["timestamp"]}

  @impl true
  def render(assigns) do
    assigns = assign(assigns, :page_size, @page_size)

    ~H"""
    <div class="list-unstyled console-text-list -tw-mx-6 tw-relative">
      <div class="tw-flex tw-px-2 tw-py-4 tw-mb-4 tw-bg-gray-800 tw-items-baseline tw-sticky tw-w-full">
        <div class="tw-font-mono tw-text-white tw-text-sm tw-space-x-2">
          {Lql.encode!(@lql_rules)}
        </div>
      </div>
      <div :if={@load_error} id="context_load_more_error" role="alert" class="tw-text-center tw-py-2">{@load_error}</div>
      <div class="tw-h-[calc(100vh-200px)] tw-overflow-y-auto tw-pr-2 tw-pl-5 -tw-ml-5" id="context_log_events" phx-hook="ScrollIntoView" data-scroll-anchor-selector="#log-events > li" phx-value-scroll-target={@target_event_id}>
        <.async_result assign={@logs}>
          <:loading>{live_react_component("Components.Loader", %{}, id: "shared-loader")}</:loading>
          <:failed>
            <div id="context_log_events_error">An error occurred.</div>
          </:failed>

          <.load_more_button id="context-load-more-before" intent="previous" target={@myself} state={EventPagination.button_state(@is_truncated_before, not is_nil(@loading_direction), @loading_direction == :previous)} label={"Load #{@page_size} more"} />
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
          <.load_more_button id="context-load-more-after" intent="next" target={@myself} state={EventPagination.button_state(@is_truncated_after, not is_nil(@loading_direction), @loading_direction == :next)} label={"Load #{@page_size} more"} />
        </.async_result>
      </div>
    </div>
    """
  end

  def highlight?(log_event, target_event_id) do
    log_event.id == target_event_id
  end

  @spec search_logs(
          String.t(),
          DateTime.t(),
          binary() | integer(),
          Lql.Rules.lql_rules(),
          Logs.Search.context_page()
        ) ::
          map()
  def search_logs(
        log_event_id,
        ts,
        source_id,
        lql_rules,
        page \\ (-@page_size - 1)..(@page_size + 1)
      ) do
    source = Sources.get_source_for_lv_param(source_id)

    so =
      %{
        lql_rules: lql_rules,
        source: source,
        tailing?: false
      }
      |> Logs.SearchOperation.new()

    case Logs.Search.search_event_context(so, log_event_id, ts, page) do
      {:ok, %{rows: rows}} ->
        %{rows: rows, source: source}

      {:error, %{error: error}} ->
        %{error: error, source: source}
    end
  end
end
