defmodule Logflare.Logs.SearchQueryExecutor do
  use GenServer
  alias __MODULE__, as: State
  use Logflare.Commons
  alias Logs.Search
  alias Logs.SearchOperation, as: SO
  import LogflareWeb.SearchLV.Utils
  alias Logflare.User.BigQueryUDFs
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
  def start_link(%RLS{source_id: source_id}, _opts \\ %{}) do
    GenServer.start_link(__MODULE__, %{source_id: source_id}, name: name(source_id))
  end

  def child_spec(%RLS{source_id: source_id} = rls) do
    %{
      id: name(source_id),
      start: {__MODULE__, :start_link, [rls]}
    }
  end

  def maybe_cancel_query(source_token) when is_atom(source_token) do
    source_token
    |> name()
    |> Process.whereis()
    |> if do
      :ok = cancel_query(source_token)
    else
      Logger.error(
        "Cancel query failed: SearchQueryExecutor process for #{source_token} not alive"
      )
    end
  end

  def maybe_execute_events_query(source_token, params) when is_atom(source_token) do
    Task.start_link(fn ->
      update_saved_search_counters(params.lql_rules, params.tailing?, params.source)
    end)

    source_token
    |> name()
    |> Process.whereis()
    |> if do
      :ok = query(params)
    else
      Logger.error("Query failed: SearchQueryExecutor process for #{source_token} not alive")
    end
  end

  def maybe_execute_agg_query(source_token, params) when is_atom(source_token) do
    source_token
    |> name()
    |> Process.whereis()
    |> if do
      :ok = query_agg(params)
    else
      Logger.error("Query failed: SearchQueryExecutor process for #{source_token} not alive")
    end
  end

  def update_saved_search_counters(lql_rules, tailing?, source) do
    qs = Lql.encode!(lql_rules)
    search = SavedSearches.get_by_qs_source_id(qs, source.id)

    search =
      if search do
        search
      else
        {:ok, search} = SavedSearches.insert(%{querystring: qs, lql_rules: lql_rules}, source)
        search
      end

    SavedSearches.inc(search.id, tailing?: tailing?)
  end

  def query(params) do
    GenServer.call(name(params.source.token), {:query, params}, @query_timeout)
  end

  def query_agg(params) do
    GenServer.call(name(params.source.token), {:query_agg, params}, @query_timeout)
  end

  def cancel_query(source_token) when is_atom(source_token) do
    GenServer.call(name(source_token), :cancel_query, @query_timeout)
  end

  def name(source_id) do
    String.to_atom("#{source_id}" <> "-search-query-server")
  end

  # Callbacks

  @impl true
  def init(%{source_id: source_id} = args) do
    Logger.debug("SearchQueryExecutor #{name(source_id)} is being initialized...")
    {:ok, args, {:continue, :after_init}}
  end

  @impl true
  def handle_continue(:after_init, state) do
    state = %__MODULE__{
      user: Users.get_by_source(state.source_id),
      agg_tasks: %{},
      event_tasks: %{},
      source_id: state.source_id
    }

    source = Sources.get_by_id_and_preload(state.source_id)
    :timer.apply_interval(1_000, __MODULE__, :start_cache_streaming_buffer_task, [source])

    {:noreply, state}
  end

  @impl true
  def handle_call({:query, params}, {lv_pid, _ref}, %State{} = state) do
    Logger.info(
      "Starting search query from #{pid_to_string(lv_pid)} for #{params.source.token} source..."
    )

    state =
      case BigQueryUDFs.create_if_not_exists_udfs_for_user_dataset(state.user) do
        {:ok, user} -> %{state | user: user}
        _ -> state
      end

    current_lv_task_params = state.event_tasks[lv_pid]

    if current_lv_task_params && current_lv_task_params[:task] do
      Logger.info(
        "SeachQueryExecutor: cancelling query task for #{pid_to_string(lv_pid)} live_view..."
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

  def handle_call({:query_agg, params}, {lv_pid, _ref}, %State{} = state) do
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
  def handle_call(:cancel_query, {lv_pid, _ref}, state) do
    current_lv_task_params = state.event_tasks[lv_pid]

    if current_lv_task_params && current_lv_task_params[:task] do
      Logger.info(
        "SeachQueryExecutor: Cancelling query task from #{pid_to_string(lv_pid)} live_view..."
      )

      Task.shutdown(current_lv_task_params.task, :brutal_kill)
    end

    event_tasks = Map.put(state.event_tasks, lv_pid, %{})

    {:reply, :ok, %{state | event_tasks: event_tasks}}
  end

  @impl true
  def handle_info({_ref, {:search_result, lv_pid, %{events: events_so}}}, state) do
    Logger.info(
      "SeachQueryExecutor: Getting search results for #{pid_to_string(lv_pid)} / #{
        state.source_id
      } source..."
    )

    {%{params: params}, new_event_tasks} = Map.pop(state.event_tasks, lv_pid)

    rows = Enum.map(events_so.rows, &LogEvent.make_from_db(&1, %{source: params.source}))

    # prevents removal of log events loaded
    # during initial tailing query
    log_events =
      params.log_events
      |> Enum.reject(& &1.is_from_stale_query)
      |> Enum.concat(rows)
      |> Enum.uniq_by(&{&1.body, &1.id})
      |> Enum.sort_by(& &1.body.timestamp, &>=/2)
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
    Logger.info(
      "SeachQueryExecutor: Getting search results for #{pid_to_string(lv_pid)} / #{
        state.source_id
      } source..."
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
    Logger.info("SearchQueryExecutor: task was shutdown")
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

    Task.async(fn ->
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

    Task.async(fn ->
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
    Task.start_link(fn ->
      source
      |> Search.query_source_streaming_buffer()
      |> case do
        {:ok, query_result} ->
          %{rows: rows} = query_result

          for row <- rows do
            le = LogEvent.make_from_db(row, %{source: source})

            LocalRepo.insert(le)
          end

          :ok

        {:error, result} ->
          Logger.warn("Streaming buffer not found for source #{source.token}")
      end
    end)
  end
end
