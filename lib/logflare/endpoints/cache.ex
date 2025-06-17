defmodule Logflare.Endpoints.Cache do
  @moduledoc """
  Handles the Endpoint caching logic.
  """

  require Logger

  alias Logflare.Endpoints
  alias Logflare.Utils.Tasks

  use GenServer, restart: :temporary

  defstruct query: nil,
            query_tasks: [],
            params: %{},
            cached_result: nil,
            disable_cache: false,
            shutdown_timer: nil,
            refresh_timer: nil

  @type t :: %__MODULE__{
          query: %Logflare.Endpoints.Query{},
          query_tasks: list(%Task{}),
          params: map(),
          cached_result: binary(),
          disable_cache: boolean(),
          shutdown_timer: reference(),
          refresh_timer: reference()
        }

  def start_link({query, params}) do
    endpoints = endpoints_part(query.id, params)

    name = {:via, :syn, {endpoints, {query.id, params}}}

    GenServer.start_link(__MODULE__, {query, params}, name: name, hibernate_after: 5_000)
  end

  @doc """
  Initiate a query. Times out at 30 seconds. BigQuery should also timeout at 25 seconds.
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

  def init({query, params}) do
    endpoints = endpoints_part(query.id)
    :syn.join(endpoints, query.id, self())

    timer = query |> cache_duration_ms() |> shutdown()

    state =
      %__MODULE__{
        query: query,
        params: params,
        shutdown_timer: timer
      }
      |> fetch_latest_query_endpoint()
      |> put_disable_cache()

    unless state.disable_cache, do: refresh(proactive_querying_ms(query))

    {:ok, state}
  end

  @doc """
  Queries BigQuery. Public because it's spawned in a task.
  """
  def handle_call(:query, _from, %__MODULE__{cached_result: nil, disable_cache: false} = state) do
    case do_query(state) do
      {:ok, result} = response ->
        state = Map.put(state, :cached_result, result)

        {:reply, response, state}

      {:error, _err} = response ->
        {:stop, :normal, response, state}
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
    {:reply, {:ok, cached_result}, state}
  end

  def handle_call(:invalidate, _from, state) do
    {:stop, :normal, {:ok, :stopped}, state}
  end

  def handle_info(:refresh, state) do
    task = Tasks.async(__MODULE__, :do_query, [state])
    tasks = [task | state.query_tasks]

    running = Enum.count(tasks)

    if running > 1,
      do: Logger.warning("CacheTaskError: #{running} Endpoints.Cache tasks are running")

    {:noreply, %{state | query_tasks: tasks}}
  end

  def handle_info(:shutdown, state) do
    Logger.debug("#{__MODULE__}: shutting down cache normally")
    {:stop, :normal, state}
  end

  def handle_info({from_task, {:ok, results}}, state) do
    tasks = Enum.reject(state.query_tasks, &(&1.pid == from_task))

    if is_reference(state.refresh_timer) do
      Process.cancel_timer(state.refresh_timer)
    end

    timer = refresh(proactive_querying_ms(state.query))

    {:noreply, %{state | cached_result: results, query_tasks: tasks, refresh_timer: timer}}
  end

  def handle_info({_from_task, {:error, _response}}, state) do
    {:stop, :normal, state}
  end

  def handle_info({:DOWN, _ref, :process, pid, :normal}, state) do
    tasks = Enum.reject(state.query_tasks, &(&1.pid == pid))

    {:noreply, %{state | query_tasks: tasks}}
  end

  def handle_info({:DOWN, _ref, :process, _pid, reason}, state) do
    Logger.warning("#{__MODULE__}: task exited with reason #{reason}")
    {:stop, :normal, state}
  end

  def do_query(state) do
    Logflare.Endpoints.run_query(state.query, state.params)
  end

  def endpoints_part(query_id, params) do
    part = :erlang.phash2({query_id, params}, System.schedulers_online())
    "endpoints_#{part}" |> String.to_existing_atom()
  end

  def endpoints_part(query_id) do
    part = :erlang.phash2(query_id, System.schedulers_online())
    "endpoints_#{part}" |> String.to_existing_atom()
  end

  defp refresh(every) do
    Process.send_after(self(), :refresh, every)
  end

  defp shutdown(every) do
    Process.send_after(self(), :shutdown, every)
  end

  defp proactive_querying_ms(query) do
    query.proactive_requerying_seconds * 1_000
  end

  defp cache_duration_ms(query) do
    query.cache_duration_seconds * 1_000
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
