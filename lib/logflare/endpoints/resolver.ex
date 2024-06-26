defmodule Logflare.Endpoints.Resolver do
  @moduledoc """
  Finds or spawns Endpoint.Cache processes across the cluster. Unique process for query plus query params.
  """
  alias Logflare.Endpoints.Cache

  require Logger

  @doc """
  Lists all caches for an endpoint
  """
  def list_caches(%Logflare.Endpoints.Query{id: id}) do
    :syn.members(:endpoints, id)
    |> Enum.map(fn {pid, _} -> pid end)
  end

  @doc """
  Starts up or performs a lookup for an Endpoint.Cache process.
  Returns the resolved pid.
  """
  def resolve(%Logflare.Endpoints.Query{id: id} = query, params) do
    :syn.lookup(:endpoints, {id, params})
    |> case do
      {pid, _} when is_pid(pid) ->
        Cache.touch(pid)
        pid

      _ ->
        spec = {Cache, {query, params}}
        Logger.debug("Starting up Endpoint.Cache for Endpoint.Query id=#{id}", endpoint_id: id)

        case DynamicSupervisor.start_child(Cache, spec) do
          {:ok, pid} -> pid
          {:error, {:already_started, pid}} -> pid
        end
    end
  end
end
