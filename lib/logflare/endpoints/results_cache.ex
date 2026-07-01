defmodule Logflare.Endpoints.ResultsCache do
  @moduledoc """
  Handles the Endpoint query results caching logic.
  """

  require Logger

  alias Logflare.Endpoints
  alias Logflare.Endpoints.EndpointQuery
  alias Logflare.Utils
  alias Logflare.Utils.Tasks

  use GenServer, restart: :temporary

  @latest_version :latest

  defstruct query_tasks: [],
            params: %{},
            opts: [],
            cached_result: nil,
            shutdown_timer: nil,
            refresh_timer: nil,
            endpoint_query_id: nil,
            endpoint_query_token: nil,
            endpoint_version_number: nil,
            parsed_labels: %{}

  @type t :: %__MODULE__{
          endpoint_query_id: integer(),
          endpoint_query_token: String.t(),
          endpoint_version_number: integer() | nil,
          query_tasks: list(%Task{}),
          params: map(),
          opts: list(),
          cached_result: map() | nil,
          shutdown_timer: reference() | nil,
          refresh_timer: reference() | nil,
          parsed_labels: map()
        }

  def start_link({query, params, opts}) do
    name = name(query, params)
    GenServer.start_link(__MODULE__, {query, params, opts}, name: name, hibernate_after: 5_000)
  end

  @doc """
  Initiate a query. Times out at 30 seconds. BigQuery should also timeout at 25 seconds.
  We have a %GoogleApi.BigQuery.V2.Model.ErrorProto{} model but it's missing fields we see in error responses.
  """
  def query(cache) when is_pid(cache) do
    GenServer.call(cache, :query, 65_000)
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
    endpoint_cache_key = {query_id, version_key} = endpoint_cache_key(query)
    endpoints = endpoints_part(query_id, version_key)
    :syn.join(endpoints, endpoint_cache_key, self())

    timer = query |> cache_duration_ms() |> shutdown()

    state =
      %__MODULE__{
        endpoint_query_id: query.id,
        endpoint_query_token: query.token,
        endpoint_version_number: query.version_number,
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
    with %EndpointQuery{} = query <- endpoint_query(state) do
      query = Map.put(query, :parsed_labels, state.parsed_labels)

      Logflare.Endpoints.run_query(query, state.params, state.opts)
      |> Utils.append_to_tuple(query)
    else
      {:error, error} ->
        {:error, error, nil}

      nil ->
        {:error, :not_found, nil}
    end
  end

  def endpoints_part(query_id, version_key) do
    endpoints_part({query_id, version_key})
  end

  def endpoints_part(query_id, version_key, params) do
    endpoints_part({query_id, version_key, params})
  end

  defp endpoints_part(partition_key) do
    part = :erlang.phash2(partition_key, System.schedulers_online())
    "endpoints_#{part}" |> String.to_existing_atom()
  end

  def name(%EndpointQuery{} = query, params) do
    param_hash = :erlang.phash2(params)
    {query_id, version_key} = endpoint_cache_key(query)

    key = {query_id, version_key, param_hash}

    {:via, :syn, {endpoints_part(query_id, version_key, params), key}}
  end

  @spec cache_partition_key(EndpointQuery.t(), map(), Keyword.t()) :: tuple()
  def cache_partition_key(%EndpointQuery{} = query, params, opts) do
    {query_id, version_key} = endpoint_cache_key(query)

    {query_id, version_key, params, opts}
  end

  def endpoint_cache_key(%EndpointQuery{id: id, version_number: version_number}),
    do: {id, version_cache_key(version_number)}

  defp version_cache_key(version_number) when is_integer(version_number),
    do: {:version, version_number}

  defp version_cache_key(_version_number), do: {:version, @latest_version}

  @spec endpoint_query(t()) :: EndpointQuery.t() | {:error, atom()} | nil
  defp endpoint_query(%__MODULE__{endpoint_version_number: version_number} = state)
       when is_integer(version_number) do
    case Endpoints.Cache.get_endpoint_query_at_version(state.endpoint_query_id, version_number) do
      {:ok, query} -> query
      {:error, error} -> {:error, error}
    end
  end

  defp endpoint_query(%__MODULE__{} = state) do
    Endpoints.Cache.get_mapped_query_by_token(state.endpoint_query_token)
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
