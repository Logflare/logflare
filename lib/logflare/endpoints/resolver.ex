defmodule Logflare.Endpoints.Resolver do
  @moduledoc """
  Finds or spawns Endpoint.Cache processes across the cluster. Unique process for query plus query params.
  """
  alias Logflare.Endpoints.ResultsCache

  require Logger
  require OpenTelemetry.Tracer

  @doc """
  Lists all caches for an endpoint across all paritions
  """
  def list_caches(%Logflare.Endpoints.Query{id: id}) do
    endpoints_partition = ResultsCache.endpoints_part(id)

    :syn.members(endpoints_partition, id)
    |> Enum.map(fn {pid, _} -> pid end)
  end

  @doc """
  Starts up or performs a lookup for an Endpoint.Cache process.
  Returns the resolved pid.
  """
  def resolve(%Logflare.Endpoints.Query{id: id} = query, params, opts) do
    attributes = %{
      "endpoint.id" => id,
      "endpoint.token" => query.token,
      "endpoint.name" => query.name,
      "endpoint.user_id" => query.user_id
    }

    ResultsCache.name(query.id, params)
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
             {Logflare.Endpoints.ResultsCache.PartitionSupervisor, {id, params, opts}}}

          case DynamicSupervisor.start_child(via, spec) do
            {:ok, pid} ->
              OpenTelemetry.Tracer.add_event(
                "logflare.endpoints.results_cache.created",
                attributes
              )

              pid

            {:error, {:already_started, pid}} ->
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
