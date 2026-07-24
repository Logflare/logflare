defmodule Logflare.Logs.SearchQueryExecutor do
  @moduledoc """
  Handles all search queries for the specific source
  """

  use GenServer
  require Logger

  import LogflareWeb.SearchLV.Utils

  alias Logflare.LogEvent
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

  def query(pid, params) do
    GenServer.call(pid, {:query, query_params(params)}, @query_timeout)
  end

  def query_page(pid, params, intent, cursor) do
    GenServer.call(
      pid,
      {:query_page, query_params(params), intent, cursor},
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
  def handle_call({:query, new_params}, {lv_pid, _ref}, state) do
    Logger.debug(
      "Starting search query from #{pid_to_string(lv_pid)} for #{state.source_id} source..."
    )

    {ref, _params} = state.event_task

    if ref do
      Logger.debug(
        "SearchQueryExecutor: cancelling query task for #{pid_to_string(lv_pid)} live_view of #{state.source_id} source..."
      )

      Task.shutdown(ref, :brutal_kill)
    end

    new_ref = start_search_task(lv_pid, new_params)

    {:reply, :ok, %{state | event_task: {new_ref, new_params}}}
  end

  def handle_call({:query_page, params, intent, cursor}, {lv_pid, _ref}, state) do
    search_op = SO.new(params)

    case SearchOperations.event_page_params(search_op, intent, cursor) do
      {:ok, page_params} ->
        {ref, _params} = state.event_task

        if ref, do: Task.shutdown(ref, :brutal_kill)

        search_op = struct(search_op, page_params)
        new_ref = start_search_task(lv_pid, search_op)

        {:reply, :ok, %{state | event_task: {new_ref, page_params}}}

      :error ->
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
    has_sentinel_row? = has_sentinel_row?(raw_rows)

    page_rows =
      raw_rows
      |> Enum.take(page_size)
      |> Enum.map(&LogEvent.make_from_db(&1, %{source: events_so.source}))
      |> uniq_sort_log_events()

    event_page_result =
      event_page_result(events_so.event_page_request, page_rows, has_sentinel_row?)

    events_so = %{
      events_so
      | rows: page_rows,
        has_more_events?: has_sentinel_row?,
        event_page_result: event_page_result
    }

    send(state.caller, {:search_result, %{events: events_so}})

    {:noreply, %{state | event_task: {nil, nil}}}
  end

  defp handle_aggregate_result(lv_pid, aggregates_so, state) do
    Logger.debug(
      "SearchQueryExecutor: Getting search aggregates for #{pid_to_string(lv_pid)} / #{state.source_id} source..."
    )

    send(state.caller, {:search_result, %{aggregates: aggregates_so}})

    {:noreply, %{state | agg_task: {nil, nil}}}
  end

  def start_search_task(lv_pid, %SO{} = so), do: do_start_search_task(lv_pid, so)

  def start_search_task(lv_pid, params) do
    do_start_search_task(lv_pid, SO.new(params))
  end

  defp do_start_search_task(lv_pid, so) do
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

  def start_aggs_task(lv_pid, params) do
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

  defp has_sentinel_row?(rows), do: length(rows) >= SearchOperations.fetch_limit()

  defp log_event_cursor(%LogEvent{} = event) do
    %{timestamp: event.body["timestamp"], id: event_id(event)}
  end

  defp event_page_result(nil, _rows, _has_more?), do: nil

  defp event_page_result(request, rows, has_more?) do
    %{
      request: request,
      has_more?: has_more?,
      cursor: page_edge_cursor(rows, SO.event_page_direction(request.intent))
    }
  end

  defp page_edge_cursor([], _direction), do: nil
  defp page_edge_cursor(rows, :previous), do: rows |> List.last() |> log_event_cursor()
  defp page_edge_cursor(rows, :next), do: rows |> List.first() |> log_event_cursor()

  defp active_task?({%Task{ref: ref}, _params}, ref), do: true
  defp active_task?({ref, _params}, ref) when is_reference(ref), do: true
  defp active_task?(_task, _ref), do: false

  defp event_id(%LogEvent{id: id, body: body}), do: id || body["id"]

  defp query_params(params) do
    Map.drop(params, [
      :range_extension_patch,
      :search_op,
      :search_op_log_events,
      :search_op_log_aggregates,
      :streams
    ])
  end
end
