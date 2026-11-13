defmodule Logflare.Endpoints.Resolver do
  @moduledoc """
  Finds or spawns Endpoint.Cache processes across the cluster. Unique process for query plus query params.
  """
  alias Logflare.Endpoints.EndpointQuery
  alias Logflare.Endpoints.ResultsCache

  require Logger
  require OpenTelemetry.Tracer

  @doc """
  Lists caches across partitions for the selected endpoint version or all versions.
  """
  @spec list_caches(EndpointQuery.t(), :selected_version | :all_versions) :: [pid()]
  def list_caches(query, scope \\ :selected_version)

  def list_caches(%EndpointQuery{id: query_id}, :all_versions),
    do: members(:core, {ResultsCache, query_id, :all_versions})

  def list_caches(%EndpointQuery{} = query, :selected_version) do
    {query_id, version_key} = cache_key = ResultsCache.endpoint_cache_key(query)
    members(ResultsCache.endpoints_part(query_id, version_key), cache_key)
  end

  @spec members(atom(), tuple()) :: [pid()]
  defp members(scope, cache_key) do
    scope
    |> :syn.members(cache_key)
    |> Enum.map(fn {pid, _} -> pid end)
  end

  @doc """
  Starts up or performs a lookup for an Endpoint.Cache process.
  Returns the resolved pid or an error after repeated registration races.
  """
  @spec resolve(EndpointQuery.t(), map(), keyword()) :: pid() | {:error, :cache_replaced}
  def resolve(%EndpointQuery{} = query, params, opts), do: resolve(query, params, opts, 3)

  @spec resolve(EndpointQuery.t(), map(), keyword(), non_neg_integer()) ::
          pid() | {:error, :cache_replaced}
  defp resolve(_query, _params, _opts, 0), do: {:error, :cache_replaced}

  defp resolve(%EndpointQuery{id: id} = query, params, opts, attempts) do
    attributes = %{
      "endpoint.id" => id,
      "endpoint.token" => query.token,
      "endpoint.name" => query.name,
      "endpoint.user_id" => query.user_id
    }

    ResultsCache.name(query, params, opts)
    |> GenServer.whereis()
    |> case do
      pid when is_pid(pid) ->
        OpenTelemetry.Tracer.add_event("logflare.endpoints.results_cache.found", attributes)
        pid

      nil ->
        OpenTelemetry.Tracer.with_span "logflare.endpoints.results_cache.create", %{
          attributes: attributes
        } do
          spec = {ResultsCache, {query, params, opts}}
          Logger.debug("Starting up Endpoint.Cache for Endpoint.Query id=#{id}", endpoint_id: id)

          via =
            {:via, PartitionSupervisor,
             {Logflare.Endpoints.ResultsCache.PartitionSupervisor,
              ResultsCache.cache_partition_key(query, params, opts)}}

          case DynamicSupervisor.start_child(via, spec) do
            {:ok, pid} ->
              OpenTelemetry.Tracer.add_event(
                "logflare.endpoints.results_cache.created",
                attributes
              )

              pid

            :ignore ->
              resolve(query, params, opts, attempts - 1)

            {:error, {:already_started, nil}} ->
              resolve(query, params, opts, attempts - 1)

            {:error, {:already_started, pid}} when is_pid(pid) ->
              OpenTelemetry.Tracer.add_event(
                "logflare.endpoints.results_cache.already_existed",
                attributes
              )

              pid
          end
        end
    end
  end
end
