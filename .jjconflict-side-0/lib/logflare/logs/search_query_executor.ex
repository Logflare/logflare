defmodule Logflare.Logs.SearchQueryExecutor do
  @moduledoc """
  Handles all search queries for the specific source
  """

  use GenServer
  require Logger

  import LogflareWeb.SearchLV.Utils

  alias Logflare.LogEvent
  alias Logflare.Logs.EventPage
  alias Logflare.Logs.Search
  alias Logflare.Logs.SearchOperation, as: SO
  alias Logflare.Logs.SearchOperations
  alias Logflare.Utils.Tasks

  @query_timeout 30_000

  # API
  def start_link(args) do
    GenServer.start_link(__MODULE__, Keyword.put(args, :caller, self()), hibernate_after: 5_000)
  end

  @impl true
  def init(args) do
    source = Keyword.get(args, :source)
    Logger.debug("SearchQueryExecutor #{source.token} is being initialized...")

    {:ok,
     %{
       caller: Keyword.get(args, :caller),
       source_token: source.token,
       user_id: source.user_id,
       source_id: source.id,
       agg_task: {nil, nil},
       event_task: {nil, nil}
     }}
  end

  @spec query(GenServer.server(), map()) :: :ok | :error
  def query(pid, params), do: query(pid, params, :initial, nil)

  @spec query(GenServer.server(), map(), EventPage.intent(), EventPage.cursor() | nil) ::
          :ok | :error
  def query(pid, params, intent, cursor) do
    GenServer.call(
      pid,
      {:query, query_params(params), intent, cursor},
      @query_timeout
    )
  end

  def query_agg(pid, params) do
    GenServer.call(pid, {:query_agg, query_params(params)}, @query_timeout)
  end

  def cancel_agg(pid) do
    GenServer.call(pid, :cancel_agg, @query_timeout)
  end

  def cancel_query(pid) do
    GenServer.call(pid, :cancel_query, @query_timeout)
  end

  # Callbacks

  @impl true
  def handle_call({:query, params, intent, cursor}, {lv_pid, _ref}, state) do
    Logger.debug(
      "Starting search query from #{pid_to_string(lv_pid)} for #{state.source_id} source..."
    )

    case SearchOperations.new_event_page(params, intent, cursor) do
      {:ok, search_op} ->
        {ref, _params} = state.event_task

        if ref do
          Logger.debug(
            "SearchQueryExecutor: cancelling query task for #{pid_to_string(lv_pid)} live_view of #{state.source_id} source..."
          )

          Task.shutdown(ref, :brutal_kill)
        end

        new_ref = start_search_task(lv_pid, search_op)

        {:reply, :ok, %{state | event_task: {new_ref, search_op.event_page_request}}}

      {:error, _reason} ->
        {:reply, :error, state}
    end
  end

  def handle_call({:query_agg, new_params}, {lv_pid, _ref}, state) do
    {ref, _params} = state.agg_task

    if ref do
      Task.shutdown(ref, :brutal_kill)
    end

    new_ref = start_aggs_task(lv_pid, new_params)

    {:reply, :ok, %{state | agg_task: {new_ref, new_params}}}
  end

  @impl true
  def handle_call(:cancel_agg, {lv_pid, _ref}, state) do
    {ref, _params} = state.agg_task

    if ref do
      Logger.debug(
        "SearchQueryExecutor: Cancelling agg task from #{pid_to_string(lv_pid)} live_view..."
      )

      Task.shutdown(ref, :brutal_kill)
    end

    {:reply, :ok, %{state | agg_task: {nil, nil}}}
  end

  @impl true
  def handle_call(:cancel_query, {lv_pid, _ref}, state) do
    {ref, _params} = state.event_task

    if ref do
      Logger.debug(
        "SearchQueryExecutor: Cancelling query task from #{pid_to_string(lv_pid)} live_view..."
      )

      Task.shutdown(ref, :brutal_kill)
    end

    {:reply, :ok, %{state | event_task: {nil, nil}}}
  end

  @impl true
  def handle_info({ref, {:search_result, lv_pid, %{events: events_so}}}, state) do
    if active_task?(state.event_task, ref) do
      handle_event_result(lv_pid, events_so, state)
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_info({ref, {:search_result, lv_pid, %{aggregates: aggregates_so}}}, state) do
    if active_task?(state.agg_task, ref) do
      handle_aggregate_result(lv_pid, aggregates_so, state)
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_info({ref, {:search_error, _lv_pid, %SO{type: :aggregates} = search_op}}, state) do
    if active_task?(state.agg_task, ref) do
      send(state.caller, {:search_error, search_op})
      {:noreply, %{state | agg_task: {nil, nil}}}
    else
      {:noreply, state}
    end
  end

  def handle_info({ref, {:search_error, _lv_pid, %SO{} = search_op}}, state) do
    if active_task?(state.event_task, ref) do
      send(state.caller, {:search_error, search_op})
      {:noreply, %{state | event_task: {nil, nil}}}
    else
      {:noreply, state}
    end
  end

  # handles task shutdown messages
  @impl true
  def handle_info({:DOWN, _, _, _, _}, state) do
    Logger.debug("SearchQueryExecutor: task was shutdown")
    {:noreply, state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("SearchQueryExecutor received unknown message: #{inspect(msg)}")
    {:noreply, state}
  end

  defp handle_event_result(lv_pid, events_so, state) do
    Logger.debug(
      "SearchQueryExecutor: Getting search events for #{pid_to_string(lv_pid)} / #{state.source_id} source..."
    )

    page_size = SearchOperations.default_limit()
    raw_rows = events_so.rows

    # indicates another page of events is available to fetch
    has_sentinel_row? = length(raw_rows) >= SearchOperations.fetch_limit()

    {page_rows, sentinel_row} =
      raw_rows
      |> Enum.map(&LogEvent.make_from_db(&1, %{source: events_so.source}))
      |> then(fn rows -> {Enum.take(rows, page_size), Enum.at(rows, page_size)} end)

    page_rows = uniq_sort_log_events(page_rows)

    request = events_so.event_page_request
    cursors = page_cursors(request, page_rows, sentinel_row)

    event_page = %EventPage{
      rows: page_rows,
      request: request,
      cursor: cursors.cursor,
      next_cursor: cursors.next_cursor,
      has_more?: has_sentinel_row?,
      events: if(request.intent == :initial, do: %{events_so | rows: page_rows})
    }

    send(state.caller, {:search_result, %{event_page: event_page}})

    {:noreply, %{state | event_task: {nil, nil}}}
  end

  defp handle_aggregate_result(lv_pid, aggregates_so, state) do
    Logger.debug(
      "SearchQueryExecutor: Getting search aggregates for #{pid_to_string(lv_pid)} / #{state.source_id} source..."
    )

    send(state.caller, {:search_result, %{aggregates: aggregates_so}})

    {:noreply, %{state | agg_task: {nil, nil}}}
  end

  defp start_search_task(lv_pid, %SO{} = so) do
    Tasks.async(fn ->
      so
      |> Search.search()
      |> case do
        {:ok, result} ->
          {:search_result, lv_pid, result}

        {:error, result} ->
          {:search_error, lv_pid, result}
      end
    end)
  end

  defp start_aggs_task(lv_pid, params) do
    so = SO.new(params)

    Tasks.async(fn ->
      so
      |> Search.aggs()
      |> case do
        {:ok, result} ->
          {:search_result, lv_pid, result}

        {:error, result} ->
          {:search_error, lv_pid, result}
      end
    end)
  end

  defp uniq_sort_log_events(log_events) do
    log_events
    |> Enum.uniq_by(&{&1.body["timestamp"], event_id(&1)})
    |> Enum.sort_by(&{&1.body["timestamp"], event_id(&1)}, :desc)
  end

  defp log_event_cursor(nil), do: nil

  defp log_event_cursor(%LogEvent{} = event) do
    %{timestamp: event.body["timestamp"], id: event_id(event)}
  end

  defp page_cursors(%{intent: :initial}, rows, sentinel_row) do
    %{
      cursor: log_event_cursor(sentinel_row),
      next_cursor: rows |> List.first() |> log_event_cursor()
    }
  end

  defp page_cursors(%{intent: :previous}, _rows, sentinel_row) do
    %{cursor: log_event_cursor(sentinel_row), next_cursor: nil}
  end

  defp page_cursors(%{intent: :next}, rows, _sentinel_row) do
    %{cursor: rows |> List.first() |> log_event_cursor(), next_cursor: nil}
  end

  defp active_task?({%Task{ref: ref}, _params}, ref), do: true
  defp active_task?(_task, _ref), do: false

  defp event_id(%LogEvent{id: id, body: body}), do: id || body["id"]

  defp query_params(params) do
    Map.drop(params, [
      :range_extension,
      :search_op,
      :search_op_log_events,
      :search_op_log_aggregates,
      :streams
    ])
  end
end
