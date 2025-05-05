defmodule Logflare.Logs.SearchQueryExecutor do
  use GenServer
  alias Logflare.Logs.Search
  alias Logflare.Logs.SearchOperation, as: SO
  import LogflareWeb.SearchLV.Utils
  alias Logflare.LogEvent
  alias Logflare.User.BigQueryUDFs
  alias Logflare.{Users, User}
  alias Logflare.Logs
  alias Logflare.Source
  alias Logflare.Backends
  alias Logflare.Utils.Tasks
  use TypedStruct
  require Logger
  @query_timeout 60_000

  @moduledoc """
  Handles all search queries for the specific source
  """

  typedstruct do
    field :source_id, atom, enforce: true
    field :user, User.t(), enforce: true
    field :event_tasks, map, enforce: true
    field :agg_tasks, map, enforce: true
  end

  # API
  def start_link(args) do
    source = Keyword.get(args, :source)

    GenServer.start_link(__MODULE__, args,
      spawn_opt: [fullsweep_after: 5_000],
      name: Backends.via_source(source, __MODULE__)
    )
  end

  @impl true
  def init(args) do
    source = Keyword.get(args, :source)
    Logger.debug("SearchQueryExecutor #{source.token} is being initialized...")

    {:ok,
     %{
       source_token: source.token,
       user_id: source.user_id,
       source_id: source.id,
       agg_tasks: %{},
       event_tasks: %{}
     }}
  end

  def maybe_cancel_query(source_token) when is_atom(source_token) do
    case Backends.lookup(__MODULE__, source_token) do
      {:ok, _} ->
        :ok = cancel_query(source_token)
        :ok = cancel_agg(source_token)

      {:error, _} ->
        :ok
    end
  end

  def maybe_execute_events_query(source_token, params) when is_atom(source_token) do
    case Backends.lookup(__MODULE__, source_token) do
      {:ok, _} ->
        :ok = query(params)

      {:error, _} ->
        Logger.error("Query failed: SearchQueryExecutor process for #{source_token} not alive")

        :error
    end
  end

  def maybe_execute_agg_query(source_token, params) when is_atom(source_token) do
    case Backends.lookup(__MODULE__, source_token) do
      {:ok, _} ->
        :ok = query_agg(params)

      {:error, _} ->
        Logger.error("Query failed: SearchQueryExecutor process for #{source_token} not alive")

        :error
    end
  end

  def query(params) do
    {:ok, pid} = Backends.lookup(__MODULE__, params.source.token)
    GenServer.call(pid, {:query, params}, @query_timeout)
  end

  def query_agg(params) do
    {:ok, pid} = Backends.lookup(__MODULE__, params.source.token)
    GenServer.call(pid, {:query_agg, params}, @query_timeout)
  end

  def cancel_agg(source_token) when is_atom(source_token) do
    {:ok, pid} = Backends.lookup(__MODULE__, source_token)
    GenServer.call(pid, :cancel_agg, @query_timeout)
  end

  def cancel_query(source_token) when is_atom(source_token) do
    {:ok, pid} = Backends.lookup(__MODULE__, source_token)
    GenServer.call(pid, :cancel_query, @query_timeout)
  end

  # Callbacks

  @impl true
  def handle_call({:query, params}, {lv_pid, _ref}, state) do
    Logger.info(
      "Starting search query from #{pid_to_string(lv_pid)} for #{params.source.token} source..."
    )

    user = Users.Cache.get(state.user_id) |> Users.Cache.preload_defaults()

    BigQueryUDFs.create_if_not_exists_udfs_for_user_dataset(user)

    current_lv_task_params = state.event_tasks[lv_pid]

    if current_lv_task_params && current_lv_task_params[:task] do
      Logger.info(
        "SearchQueryExecutor: cancelling query task for #{pid_to_string(lv_pid)} live_view..."
      )

      Task.shutdown(current_lv_task_params.task, :brutal_kill)
    end

    event_tasks =
      Map.put(state.event_tasks, lv_pid, %{
        task: start_search_task(lv_pid, params),
        params: params
      })

    {:reply, :ok, %{state | event_tasks: event_tasks}}
  end

  def handle_call({:query_agg, params}, {lv_pid, _ref}, state) do
    current_lv_task_params = state.agg_tasks[lv_pid]

    if current_lv_task_params && current_lv_task_params[:task] do
      Task.shutdown(current_lv_task_params.task, :brutal_kill)
    end

    agg_tasks =
      Map.put(state.agg_tasks, lv_pid, %{
        task: start_aggs_task(lv_pid, params),
        params: params
      })

    {:reply, :ok, %{state | agg_tasks: agg_tasks}}
  end

  @impl true
  def handle_call(:cancel_agg, {lv_pid, _ref}, state) do
    current_lv_task_params = state.agg_tasks[lv_pid]

    if current_lv_task_params && current_lv_task_params[:task] do
      Logger.info(
        "SearchQueryExecutor: Cancelling agg task from #{pid_to_string(lv_pid)} live_view..."
      )

      Task.shutdown(current_lv_task_params.task, :brutal_kill)
    end

    agg_tasks = Map.put(state.agg_tasks, lv_pid, %{})

    {:reply, :ok, %{state | agg_tasks: agg_tasks}}
  end

  @impl true
  def handle_call(:cancel_query, {lv_pid, _ref}, state) do
    current_lv_task_params = state.event_tasks[lv_pid]

    if current_lv_task_params && current_lv_task_params[:task] do
      Logger.info(
        "SearchQueryExecutor: Cancelling query task from #{pid_to_string(lv_pid)} live_view..."
      )

      Task.shutdown(current_lv_task_params.task, :brutal_kill)
    end

    event_tasks = Map.put(state.event_tasks, lv_pid, %{})

    {:reply, :ok, %{state | event_tasks: event_tasks}}
  end

  @impl true
  def handle_info({_ref, {:search_result, lv_pid, %{events: events_so}}}, state) do
    Logger.debug(
      "SearchQueryExecutor: Getting search events for #{pid_to_string(lv_pid)} / #{state.source_id} source..."
    )

    {%{params: params}, new_event_tasks} = Map.pop(state.event_tasks, lv_pid)

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

    maybe_send(
      lv_pid,
      {:search_result,
       %{
         events: %{events_so | rows: log_events}
       }}
    )

    {:noreply, %{state | event_tasks: new_event_tasks}}
  end

  @impl true
  def handle_info({_ref, {:search_result, lv_pid, %{aggregates: aggregates_so}}}, state) do
    Logger.debug(
      "SearchQueryExecutor: Getting search aggregates for #{pid_to_string(lv_pid)} / #{state.source_id} source..."
    )

    {_, new_agg_tasks} = Map.pop(state.agg_tasks, lv_pid)

    maybe_send(
      lv_pid,
      {:search_result,
       %{
         aggregates: aggregates_so
       }}
    )

    {:noreply, %{state | agg_tasks: new_agg_tasks}}
  end

  @impl true
  def handle_info({_ref, {:search_error, lv_pid, %SO{} = search_op}}, state) do
    maybe_send(lv_pid, {:search_error, search_op})
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
    Logger.error("SearchQueryExecutor received unknown message: #{inspect(msg)}")
    {:noreply, state}
  end

  def maybe_send(lv_pid, msg) do
    if Process.alive?(lv_pid) do
      send(lv_pid, msg)
    else
      Logger.info(
        "SearchQueryExecutor not sending msg to #{pid_to_string(lv_pid)} because it's not alive} "
      )
    end
  end

  def start_search_task(lv_pid, params) do
    so = SO.new(params)

    if so.tailing? do
      start_cache_streaming_buffer_task(so.source)
    end

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

  def start_cache_streaming_buffer_task(%Source{} = source) do
    Tasks.start_child(fn ->
      source
      |> Search.query_source_streaming_buffer()
      |> case do
        {:ok, query_result} ->
          %{rows: rows} = query_result

          for row <- rows do
            le = LogEvent.make_from_db(row, %{source: source})

            Logs.LogEvents.Cache.put(
              source.token,
              {"uuid", le.id},
              le
            )
          end

          :ok

        {:error, _result} ->
          Logger.warning("Streaming buffer not found for source #{source.token}")
      end
    end)
  end
end
