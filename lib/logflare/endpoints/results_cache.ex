defmodule Logflare.Endpoints.ResultsCache do
  @moduledoc """
  Handles the Endpoint query results caching logic.
  """

  require Logger

  alias Logflare.Endpoints
  alias Logflare.Endpoints.EndpointQuery
  alias Logflare.Utils

  use GenServer, restart: :temporary

  @latest_version :latest

  defstruct query_tasks: [],
            waiting_callers: [],
            params: %{},
            opts: [],
            cached_result: nil,
            cache_name: nil,
            draining?: false,
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
          waiting_callers: [GenServer.from()],
          params: map(),
          opts: list(),
          cached_result: map() | nil,
          cache_name: tuple(),
          draining?: boolean(),
          shutdown_timer: reference() | nil,
          refresh_timer: reference() | nil,
          parsed_labels: map()
        }

  def start_link({query, params, opts}) do
    GenServer.start_link(__MODULE__, {query, params, opts}, hibernate_after: 5_000)
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

    :exit, {reason, _call} when reason in [:noproc, :normal] ->
      {:error, :stale_cache}

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
  Removes the cache from lookup, allowing pending foreground responses to finish before stopping.
  """
  @spec invalidate(pid()) :: {:ok, :stopped | :draining} | {:error, term()}
  def invalidate(cache) when is_pid(cache) do
    GenServer.call(cache, :invalidate)
  catch
    :exit, {:noproc, _call} -> {:ok, :stopped}
    :exit, {:normal, _call} -> {:ok, :stopped}
    :exit, reason -> {:error, reason}
  end

  @impl true
  def init({query, params, opts}) do
    {:via, :syn, {scope, key}} = name(query, params, opts)

    case :syn.register(scope, key, self()) do
      :ok -> init_cache(query, params, opts)
      {:error, :taken} -> :ignore
    end
  end

  @spec init_cache(EndpointQuery.t(), map(), keyword()) :: {:ok, t()}
  defp init_cache(query, params, opts) do
    endpoint_cache_key = {query_id, version_key} = endpoint_cache_key(query)
    endpoints = endpoints_part(query_id, version_key)
    :syn.join(endpoints, endpoint_cache_key, self())
    :syn.join(:core, {__MODULE__, query_id, :all_versions}, self())

    timer = query |> cache_duration_ms() |> shutdown()
    refresh_timer = if disable_cache?(query), do: nil, else: refresh(proactive_querying_ms(query))

    state =
      %__MODULE__{
        cache_name: name(query, params, opts),
        endpoint_query_id: query.id,
        endpoint_query_token: query.token,
        endpoint_version_number: query.version_number,
        params: params,
        opts: opts,
        shutdown_timer: timer,
        refresh_timer: refresh_timer,
        parsed_labels: query.parsed_labels
      }

    {:ok, state}
  end

  @impl true
  def handle_call(:query, _from, %__MODULE__{draining?: true} = state),
    do: {:reply, {:error, :stale_cache}, state}

  def handle_call(:query, from, %__MODULE__{cached_result: nil} = state) do
    state = %{state | waiting_callers: [from | state.waiting_callers]}
    {:noreply, start_query(state)}
  end

  def handle_call(:query, _from, %__MODULE__{cached_result: cached_result} = state) do
    {:reply, {:ok, cached_result}, state}
  end

  def handle_call(:invalidate, _from, %__MODULE__{draining?: true} = state),
    do: {:reply, {:ok, :draining}, state}

  def handle_call(:invalidate, _from, state) do
    {:via, :syn, {scope, key}} = state.cache_name
    :ok = :syn.unregister(scope, key)

    {query_id, version_key} =
      cache_key = {state.endpoint_query_id, version_cache_key(state.endpoint_version_number)}

    :syn.leave(endpoints_part(query_id, version_key), cache_key, self())
    :syn.leave(:core, {__MODULE__, query_id, :all_versions}, self())
    cancel_timer(state.refresh_timer)
    cancel_timer(state.shutdown_timer)

    state = %{
      state
      | draining?: true,
        cached_result: nil,
        refresh_timer: nil,
        shutdown_timer: nil
    }

    if state.waiting_callers == [] do
      {:stop, :normal, {:ok, :stopped}, state}
    else
      {:reply, {:ok, :draining}, state}
    end
  end

  @impl true
  def handle_info(:refresh, %__MODULE__{draining?: true} = state), do: {:noreply, state}
  def handle_info(:refresh, state), do: {:noreply, start_query(state)}

  def handle_info(:shutdown, %__MODULE__{cached_result: nil, waiting_callers: [_ | _]} = state),
    do: {:noreply, %{state | shutdown_timer: nil}}

  def handle_info(:shutdown, state) do
    Logger.debug("#{__MODULE__}: shutting down cache normally")
    {:stop, :normal, state}
  end

  def handle_info({ref, {:ok, results, query}}, state) when is_reference(ref) do
    state = complete_query(state, ref, {:ok, results})

    cancel_timer(state.refresh_timer)

    if state.draining? or disable_cache?(query) or is_nil(state.shutdown_timer) do
      {:stop, :normal, state}
    else
      timer = refresh(proactive_querying_ms(query))
      {:noreply, %{state | cached_result: results, refresh_timer: timer}}
    end
  end

  def handle_info({ref, {:error, response, _query}}, state) when is_reference(ref) do
    {:stop, :normal, complete_query(state, ref, {:error, response})}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    Logger.warning("#{__MODULE__}: task exited with reason #{inspect(reason)}")
    {:stop, :normal, complete_query(state, ref, {:error, stopped_error()})}
  end

  @impl true
  def terminate(_reason, state) do
    Enum.each(state.query_tasks, &Task.shutdown(&1, :brutal_kill))
    reply_waiting_callers(state, {:error, stopped_error()})
  end

  @spec cancel_timer(reference() | nil) :: :ok
  defp cancel_timer(timer) do
    if is_reference(timer), do: Process.cancel_timer(timer)
    :ok
  end

  @spec start_query(t()) :: t()
  defp start_query(%__MODULE__{query_tasks: []} = state) do
    task =
      Task.Supervisor.async_nolink(
        {:via, PartitionSupervisor, {Logflare.TaskSupervisors, self()}},
        __MODULE__,
        :do_query,
        [state]
      )

    %{state | query_tasks: [task]}
  end

  defp start_query(state), do: state

  @spec complete_query(t(), reference(), term()) :: t()
  defp complete_query(state, ref, response) do
    Process.demonitor(ref, [:flush])
    reply_waiting_callers(state, response)
    %{state | query_tasks: Enum.reject(state.query_tasks, &(&1.ref == ref)), waiting_callers: []}
  end

  @spec reply_waiting_callers(t(), term()) :: :ok
  defp reply_waiting_callers(state, response),
    do: Enum.each(state.waiting_callers, &GenServer.reply(&1, response))

  @spec stopped_error() :: map()
  defp stopped_error do
    %{
      "code" => 503,
      "errors" => [],
      "status" => "CACHE_STOPPED",
      "message" => "Query cache stopped before execution completed. Retry the request."
    }
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

  def name(query, params, opts \\ [])

  def name(%EndpointQuery{} = query, params, opts) do
    param_hash = :crypto.hash(:sha256, :erlang.term_to_binary(params))
    opts_hash = :crypto.hash(:sha256, :erlang.term_to_binary(opts))
    {query_id, version_key} = endpoint_cache_key(query)

    key = {query_id, version_key, param_hash, opts_hash}

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
    with %EndpointQuery{} = current <- Endpoints.Cache.get_endpoint_query(state.endpoint_query_id),
         {:ok, query} <-
           Endpoints.Cache.get_endpoint_query_at_version(state.endpoint_query_id, version_number) do
      %{
        query
        | sandboxable: current.sandboxable,
          enforced_clickhouse_settings: current.enforced_clickhouse_settings
      }
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
