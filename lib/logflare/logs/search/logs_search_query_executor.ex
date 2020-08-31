defmodule Logflare.Logs.SearchQueryExecutor do
  use GenServer
  alias __MODULE__, as: State
  alias Logflare.Logs.Search
  alias Logflare.Logs.SearchOperation, as: SO
  import LogflareWeb.SearchLV.Utils
  alias Logflare.LogEvent
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.User.BigQueryUDFs
  alias Logflare.{Users, User}
  alias Logflare.Logs
  alias Logflare.Source
  alias Logflare.SavedSearches
  alias Logflare.Lql
  use TypedStruct
  require Logger
  @query_timeout 60_000

  @moduledoc """
  Handles all search queries for the specific source
  """

  typedstruct do
    field :source_id, atom, enforce: true
    field :user, User.t(), enforce: true
    field :query_tasks, map, enforce: true
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

  def maybe_execute_query(source_token, params) when is_atom(source_token) do
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
      query_tasks: %{},
      source_id: state.source_id
    }

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

    current_lv_task_params = state.query_tasks[lv_pid]

    if current_lv_task_params && current_lv_task_params[:task] do
      Logger.info(
        "SeachQueryExecutor: cancelling query task for #{pid_to_string(lv_pid)} live_view..."
      )

      Task.shutdown(current_lv_task_params.task, :brutal_kill)
    end

    query_tasks =
      Map.put(state.query_tasks, lv_pid, %{
        task: start_task(lv_pid, params),
        params: params
      })

    {:reply, :ok, %{state | query_tasks: query_tasks}}
  end

  @impl true
  def handle_call(:cancel_query, {lv_pid, _ref}, state) do
    current_lv_task_params = state.query_tasks[lv_pid]

    if current_lv_task_params && current_lv_task_params[:task] do
      Logger.info(
        "SeachQueryExecutor: Cancelling query task from #{pid_to_string(lv_pid)} live_view..."
      )

      Task.shutdown(current_lv_task_params.task, :brutal_kill)
    end

    query_tasks = Map.put(state.query_tasks, lv_pid, %{})

    {:reply, :ok, %{state | query_tasks: query_tasks}}
  end

  @impl true
  def handle_info({_ref, {:search_result, lv_pid, result}}, state) do
    Logger.info(
      "SeachQueryExecutor: Getting search results for #{pid_to_string(lv_pid)} / #{
        state.source_id
      } source..."
    )

    %{events: events_so, aggregates: aggregates_so} = result

    {%{params: params}, new_query_tasks} = Map.pop(state.query_tasks, lv_pid)

    rows = Enum.map(events_so.rows, &LogEvent.make_from_db(&1, %{source: params.source}))

    # prevents removal of log events loaded
    # during initial tailing query
    log_events =
      params.log_events
      |> Enum.reject(& &1.is_from_stale_query?)
      |> Enum.concat(rows)
      |> Enum.uniq_by(&{&1.body, &1.id})
      |> Enum.sort_by(& &1.body.timestamp, &>=/2)
      |> Enum.take(100)

    maybe_send(
      lv_pid,
      {:search_result,
       %{
         events: %{events_so | rows: log_events},
         aggregates: aggregates_so
       }}
    )

    state = %{state | query_tasks: new_query_tasks}
    {:noreply, state}
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

  def start_task(lv_pid, params) do
    so = SO.new(params)

    if so.tailing? do
      start_cache_streaming_buffer_task(so.source)
    end

    start_search_and_aggs_task(so, lv_pid)
  end

  def process_search_response(tup, lv_pid, type) when type in ~w(events aggregates)a do
    case tup do
      {:ok, search_op} ->
        {:search_result, type, lv_pid, search_op}

      {:error, err} ->
        {:search_error, type, lv_pid, err}
    end
  end

  def start_search_and_aggs_task(%SO{} = so, lv_pid) do
    Task.async(fn ->
      so
      |> Search.search_and_aggs()
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

            Logs.LogEvents.Cache.put_event_with_id_and_timestamp(
              source.token,
              [id: le.id, timestamp: DateTime.from_unix!(le.body.timestamp, :microsecond)],
              le
            )
          end

          :ok

        {:error, _result} ->
          Logger.warn("Streaming buffer not found for source #{source.token}")
      end
    end)
  end
end
