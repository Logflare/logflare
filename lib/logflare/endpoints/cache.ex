defmodule Logflare.Endpoints.Cache do
  @moduledoc """
  Handles the Endpoint caching logic.
  """

  require Logger
  alias Logflare.Endpoints
  use GenServer, restart: :temporary

  defstruct query: nil,
            query_tasks: [],
            params: %{},
            last_query_at: nil,
            last_update_at: nil,
            cached_result: nil,
            disable_cache: false

  @type t :: %__MODULE__{
          query: %Logflare.Endpoints.Query{},
          query_tasks: list(%Task{}),
          params: map(),
          last_query_at: DateTime.t(),
          last_update_at: DateTime.t(),
          cached_result: Logflare.BqRepo.results(),
          disable_cache: boolean()
        }

  def start_link({query, params}) do
    GenServer.start_link(__MODULE__, {query, params})
  end

  @doc """
  Initiate a query. Times out at 30 seconds. BigQuery should also timeout at 60 seconds.
  We have a %GoogleApi.BigQuery.V2.Model.ErrorProto{} model but it's missing fields we see in error responses.
  """
  def query(cache) when is_pid(cache) do
    GenServer.call(cache, :query, 30_000)
  catch
    :exit, {:timeout, _call} ->
      Logger.warning("Endpoint query timeout")

      message = """
      Backend query timeout! Optimizing your query will help. Some tips:

      - `select` fewer columns. Only columns in the `select` statement are scanned.
      - Narrow the date range - e.g `where timestamp > timestamp_sub(current_timestamp, interval 1 hour)`.
      - Aggregate data. Analytics databases are designed to perform well when using aggregate functions.
      - Run the query again. This error could be intermittent.

      If you continue to see this error please contact support.
      """

      err = %{
        "code" => 504,
        "errors" => [],
        "message" => message,
        "status" => "TIMEOUT"
      }

      {:error, err}

    :exit, reason ->
      Logger.error("Endpoint query exited for an unknown reason", error_string: inspect(reason))

      err = %{
        "code" => 502,
        "errors" => [],
        "message" =>
          "Something went wrong! Unknown error. If this continues please contact support.",
        "status" => "UNKNOWN"
      }

      {:error, err}
  end

  @doc """
  Invalidates the cache by stopping the cache process.
  """
  def invalidate(cache) when is_pid(cache) do
    GenServer.call(cache, :invalidate)
  end

  @doc """
  Updates the `last_query_at` key in the process state to act as if a query was recently made.
  """
  def touch(cache) when is_pid(cache) do
    GenServer.cast(cache, :touch)
  end

  def init({query, params}) do
    state =
      %__MODULE__{
        query: query,
        params: params,
        last_update_at: DateTime.utc_now(),
        last_query_at: DateTime.utc_now()
      }
      |> fetch_latest_query_endpoint()
      |> put_disable_cache()

    unless state.disable_cache, do: refresh(proactive_querying_ms(state))

    # register on syn
    pid = self()
    :syn.register(:endpoints, {query.id, params}, pid)
    :syn.join(:endpoints, query.id, pid)

    {:ok, state}
  end

  @doc """
  Queries BigQuery. Public because it's spawned in a task.
  """
  def handle_call(:query, _from, %__MODULE__{cached_result: nil, disable_cache: false} = state) do
    case do_query(state) do
      {:ok, result} = response ->
        state =
          state
          |> Map.put(:last_query_at, DateTime.utc_now())
          |> Map.put(:cached_result, result)

        {:reply, response, state}

      {:error, _err} = response ->
        {:reply, response, state}
    end
  end

  def handle_call(:query, _from, %__MODULE__{cached_result: nil, disable_cache: true} = state) do
    case do_query(state) do
      {:ok, _result} = response ->
        {:stop, :normal, response, state}

      {:error, _err} = response ->
        {:stop, :normal, response, state}
    end
  end

  def handle_call(:query, _from, %__MODULE__{cached_result: cached_result} = state) do
    state =
      state
      |> Map.put(:last_query_at, DateTime.utc_now())

    {:reply, {:ok, cached_result}, state}
  end

  def handle_call(:invalidate, _from, state) do
    {:stop, :normal, {:ok, :stopped}, state}
  end

  def handle_cast(:touch, state) do
    state = %{state | last_query_at: DateTime.utc_now()}
    {:noreply, state}
  end

  def handle_info(:refresh, state) do
    task = Task.async(__MODULE__, :do_query, [state])
    tasks = [task | state.query_tasks]

    {:noreply, %{state | query_tasks: tasks}}
  end

  def handle_info({_from_task, {:ok, results}}, state) do
    {:noreply, %{state | cached_result: results}}
  end

  def handle_info({_from_task, {:error, _response}}, state) do
    {:stop, :normal, state}
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    tasks = Enum.reject(state.query_tasks, &(&1.pid == pid))

    cond do
      since_last_query(state) < state.query.cache_duration_seconds ->
        refresh(proactive_querying_ms(state))
        {:noreply, %{state | query_tasks: tasks}}

      Enum.empty?(tasks) ->
        {:stop, :normal, state}

      true ->
        {:stop, :normal, state}
    end
  end

  def do_query(state) do
    Logflare.Endpoints.run_query(state.query, state.params)
  end

  defp refresh(every) do
    Process.send_after(self(), :refresh, every)
  end

  defp since_last_query(state) do
    DateTime.diff(DateTime.utc_now(), state.last_query_at, :second)
  end

  defp proactive_querying_ms(state) do
    state.query.proactive_requerying_seconds * 1_000
  end

  defp fetch_latest_query_endpoint(state) do
    %{
      state
      | query:
          Endpoints.get_mapped_query_by_token(state.query.token)
          |> Logflare.Repo.preload(:user)
    }
  end

  defp put_disable_cache(state) do
    if state.query.cache_duration_seconds == 0,
      do: %{state | disable_cache: true},
      else: %{state | disable_cache: false}
  end
end
