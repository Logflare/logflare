defmodule Logflare.Logs.SearchQueryExecutor do
  use GenServer
  alias Logflare.Logs.Search
  alias Logflare.Logs.SearchOperations.SearchOperation, as: SO
  import LogflareWeb.SearchLV.Utils
  alias Logflare.LogEvent
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.User.BigQueryUDFs
  alias Logflare.{Users, User}
  require Logger
  @query_timeout 60_000

  @moduledoc """
  Handles all search queries for the specific source
  """

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
      Logger.error("Cancel query failed: SearchQueryExecutor process for not alive")
    end
  end

  def maybe_execute_query(source_token, params) when is_atom(source_token) do
    source_token
    |> name()
    |> Process.whereis()
    |> if do
      :ok = query(params)
    else
      Logger.error("Query failed: SearchQueryExecutor process for not alive")
    end
  end

  def query(params) do
    do_query(params)
  end

  def cancel_query(source_token) when is_atom(source_token) do
    GenServer.call(name(source_token), :cancel_query, @query_timeout)
  end

  def name(source_id) do
    String.to_atom("#{source_id}" <> "-search-query-server")
  end

  # Private API

  defp do_query(params) do
    GenServer.call(name(params.source.token), {:query, params}, @query_timeout)
  end

  # Callbacks

  @impl true
  def init(%{source_id: source_id} = args) do
    Logger.debug("SearchQueryExecutor #{name(source_id)} is being initialized...")
    {:ok, Map.merge(args, %{query_tasks: %{}})}
  end

  @impl true
  def handle_call({:query, params}, {lv_pid, _ref}, state) do
    Logger.info(
      "Starting search query from #{pid_to_string(lv_pid)} for #{params.source.token} source..."
    )

    BigQueryUDFs.create_if_not_exists_udfs_for_user_dataset(state.user)

    current_lv_task_params = state.query_tasks[lv_pid]

    if current_lv_task_params && current_lv_task_params[:task] do
      Logger.info(
        "SeachQueryExecutor: cancelling query task for #{pid_to_string(lv_pid)} live_view..."
      )

      Task.shutdown(current_lv_task_params.task, :brutal_kill)
    end

    state =
      put_in(state, [:query_tasks, lv_pid], %{
        task: start_task(lv_pid, params),
        params: params
      })

    {:reply, :ok, state}
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

    state = put_in(state, [:query_tasks, lv_pid], %{})

    {:reply, :ok, state}
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

    rows =
      events_so
      |> Map.get(:rows)
      |> Enum.map(&LogEvent.make_from_db(&1, %{source: params.source}))

    # prevents removal of log events loaded
    # during initial tailing query
    log_events =
      params.log_events
      |> Enum.concat(rows)
      |> Enum.uniq_by(& &1.body)
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
    Task.async(fn ->
      SO
      |> struct(params)
      |> Search.search_and_aggs()
      |> case do
        {:ok, result} ->
          {:search_result, lv_pid, result}

        {:error, result} ->
          {:search_error, lv_pid, result}
      end
    end)
  end

  def process_search_response(tup, lv_pid, type) when type in ~w(events aggregates)a do
    case tup do
      {:ok, search_op} ->
        {:search_result, type, lv_pid, search_op}

      {:error, err} ->
        {:search_error, type, lv_pid, err}
    end
  end
end
