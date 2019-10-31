defmodule Logflare.Logs.SearchQueryExecutor do
  use GenServer
  alias Logflare.Logs.Search
  alias Logflare.Logs.SearchOperations.SearchOperation, as: SO
  import Logflare.Logs.Search.Utils
  alias Logflare.LogEvent
  alias Logflare.Source.RecentLogsServer, as: RLS
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

  def query(params) do
    do_query(params)
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
  def init(args) do
    {:ok, Map.merge(args, %{query_tasks: %{}})}
  end

  @impl true
  def handle_call({:query, params}, {lv_pid, _ref}, state) do
    Logger.info(
      "Starting search query from #{pid_to_string(lv_pid)} for #{params.source.token} source..."
    )

    current_lv_task_params = state.query_tasks[lv_pid]
    if current_lv_task_params, do: Task.shutdown(current_lv_task_params.task, :brutal_kill)

    state =
      put_in(state, [:query_tasks, lv_pid], %{
        task: start_task(lv_pid, params),
        params: params
      })

    {:reply, :ok, state}
  end

  @impl true
  def handle_info({_ref, {:search_result, lv_pid, so}}, state) do
    {%{params: params}, new_query_tasks} = Map.pop(state.query_tasks, lv_pid)

    rows =
      so
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

    maybe_send(lv_pid, {:search_result, %{so | rows: log_events}})

    state = %{state | query_tasks: new_query_tasks}
    {:noreply, state}
  end

  @impl true
  def handle_info({_ref, {:search_error, lv_pid, %SO{} = search_op}}, state) do
    maybe_send(lv_pid, {:search_error, search_op})
    {:noreply, state}
  end

  @impl true
  # handles task shutdown messages
  def handle_info(_task, state), do: {:noreply, state}

  def maybe_send(lv_pid, msg) do
    if Process.alive?(lv_pid) do
      send(lv_pid, msg)
    else
      Logger.info("Not sending msg to #{pid_to_string(lv_pid)} because it's not alive} ")
    end
  end

  def start_task(lv_pid, params) do
    Task.async(fn ->
      SO
      |> struct(params)
      |> Search.search()
      |> case do
        {:ok, search_op} ->
          {:search_result, lv_pid, search_op}

        {:error, err} ->
          {:search_error, lv_pid, err}
      end
    end)
  end
end
