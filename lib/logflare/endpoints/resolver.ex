defmodule Logflare.Endpoints.Resolver do
  @moduledoc """
  Finds or spawns Endpoint.Cache processes across the cluster. Unique process for query plus query params.
  """
  alias Logflare.Endpoints.Cache

  def resolve(%Logflare.Endpoints.Query{id: id}) do
    :global.registered_names()
    |> Enum.filter(fn
      {Cache, ^id, _} -> true
      _ -> false
    end)
    |> Enum.map(&:global.whereis_name/1)
  end

  def resolve(%Logflare.Endpoints.Query{id: id} = query, params) do
    :global.set_lock({Cache, {id, params}})

    result =
      case :global.whereis_name({Cache, id, params}) do
        :undefined ->
          spec = {Cache, {query, params}}

          case DynamicSupervisor.start_child(Cache, spec) do
            {:ok, pid} -> pid
            {:error, {:already_started, pid}} -> pid
          end

        pid ->
          Cache.touch(pid)
          pid
      end

    :global.del_lock({Cache, {id, params}})

    result
  end
end
