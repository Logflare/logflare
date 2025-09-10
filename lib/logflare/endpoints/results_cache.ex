defmodule Logflare.Endpoints.ResultsCache do
  @moduledoc """
  Handles the Endpoint query results caching logic.
  """

  require Logger

  alias Logflare.Endpoints
  alias Logflare.Utils.Tasks

  use GenServer, restart: :temporary

  defstruct query_tasks: [],
            params: %{},
            opts: [],
            cached_result: nil,
            shutdown_timer: nil,
            refresh_timer: nil,
            endpoint_query_id: nil,
            endpoint_query_token: nil,
            parsed_labels: %{}

  @type t :: %__MODULE__{
          endpoint_query_id: integer(),
          endpoint_query_token: String.t(),
          query_tasks: list(%Task{}),
          params: map(),
          opts: list(),
          cached_result: binary(),
          shutdown_timer: reference(),
          refresh_timer: reference(),
          parsed_labels: map()
        }

  def start_link({query, params, opts}) do
    name = name(query.id, params)
    GenServer.start_link(__MODULE__, {query, params, opts}, name: name, hibernate_after: 5_000)
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

  def init({query, params, opts}) do
    endpoints = endpoints_part(query.id)
    :syn.join(endpoints, query.id, self())

    timer = query |> cache_duration_ms() |> shutdown()

    state =
      %__MODULE__{
        endpoint_query_id: query.id,
        endpoint_query_token: query.token,
        params: params,
        opts: opts,
        shutdown_timer: timer,
        parsed_labels: query.parsed_labels
      }

    unless disable_cache?(query), do: refresh(proactive_querying_ms(query))

    {:ok, state}
  end

  @doc """
  Queries BigQuery. Public because it's spawned in a task.
  """
  def handle_call(:query, _from, %__MODULE__{cached_result: nil} = state) do
    case do_query(state) do
      {:ok, result, query} ->
        state = Map.put(state, :cached_result, result)
        response = {:ok, result}

        if disable_cache?(query) do
          {:stop, :normal, response, state}
        else
          {:reply, response, state}
        end

      {:error, err, _query} ->
        {:stop, :normal, {:error, err}, state}
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
      do: Logger.warning("CacheTaskError: #{running} Endpoints.ResultsCache tasks are running")

    {:noreply, %{state | query_tasks: tasks}}
  end

  def handle_info(:shutdown, state) do
    Logger.debug("#{__MODULE__}: shutting down cache normally")
    {:stop, :normal, state}
  end

  def handle_info({from_task, {:ok, results, query}}, state) do
    tasks = Enum.reject(state.query_tasks, &(&1.pid == from_task))

    if is_reference(state.refresh_timer) do
      Process.cancel_timer(state.refresh_timer)
    end

    timer = refresh(proactive_querying_ms(query))
    new_state = %{state | cached_result: results, query_tasks: tasks, refresh_timer: timer}

    if disable_cache?(query) do
      {:stop, :normal, new_state}
    else
      {:noreply, new_state}
    end
  end

  def handle_info({_from_task, {:error, _response, _query}}, state) do
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
    query =
      Endpoints.Cache.get_mapped_query_by_token(state.endpoint_query_token)
      |> Map.put(:parsed_labels, state.parsed_labels)

    Logflare.Endpoints.run_query(query, state.params, state.opts)
    |> Tuple.append(query)
  end

  def endpoints_part(query_id, params) do
    part = :erlang.phash2({query_id, params}, System.schedulers_online())
    "endpoints_#{part}" |> String.to_existing_atom()
  end

  def endpoints_part(query_id) do
    part = :erlang.phash2(query_id, System.schedulers_online())
    "endpoints_#{part}" |> String.to_existing_atom()
  end

  def name(query_id, params) do
    partition = endpoints_part(query_id, params)
    param_hash = :erlang.phash2(params)
    {:via, :syn, {partition, {query_id, param_hash}}}
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

  defp disable_cache?(query) do
    query.cache_duration_seconds == 0
  end
end
