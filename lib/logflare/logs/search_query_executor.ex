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
  alias Logflare.Utils.Tasks

  @query_timeout 60_000

  # API
  def start_link(args) do
    GenServer.start_link(__MODULE__, Keyword.put(args, :caller, self()),
      spawn_opt: [fullsweep_after: 5_000],
      hibernate_after: 5_000
    )
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
    GenServer.call(pid, {:query, params}, @query_timeout)
  end

  def query_agg(pid, params) do
    GenServer.call(pid, {:query_agg, params}, @query_timeout)
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
  def handle_info({_ref, {:search_result, lv_pid, %{events: events_so}}}, state) do
    Logger.debug(
      "SearchQueryExecutor: Getting search events for #{pid_to_string(lv_pid)} / #{state.source_id} source..."
    )

    {_ref, params} = state.event_task

    rows = Enum.map(events_so.rows, &LogEvent.make_from_db(&1, %{source: params.source}))

    old_rows = if params.search_op_log_events, do: params.search_op_log_events.rows, else: []

    # prevents removal of log events loaded
    # during initial tailing query
    log_events =
      old_rows
      |> Enum.reject(& &1.is_from_stale_query)
      |> Enum.concat(rows)
      |> Enum.uniq_by(&{&1.body, &1.id})
      |> Enum.sort_by(& &1.body["timestamp"], &>=/2)
      |> Enum.take(100)

    send(
      state.caller,
      {:search_result,
       %{
         events: %{events_so | rows: log_events}
       }}
    )

    {:noreply, %{state | event_task: {nil, nil}}}
  end

  @impl true
  def handle_info({_ref, {:search_result, lv_pid, %{aggregates: aggregates_so}}}, state) do
    Logger.debug(
      "SearchQueryExecutor: Getting search aggregates for #{pid_to_string(lv_pid)} / #{state.source_id} source..."
    )

    {_ref, _params} = state.agg_task

    send(
      state.caller,
      {:search_result,
       %{
         aggregates: aggregates_so
       }}
    )

    {:noreply, %{state | agg_task: {nil, nil}}}
  end

  @impl true
  def handle_info({_ref, {:search_error, _lv_pid, %SO{} = search_op}}, state) do
    send(state.caller, {:search_error, search_op})
    {:noreply, state}
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

  def start_search_task(lv_pid, params) do
    so = SO.new(params)

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
end
