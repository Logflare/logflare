defmodule Logflare.Repo.Replicas do
  @moduledoc """
  Manages a pool of PostgreSQL read replica connections for `Logflare.Repo`.

  When started with one or more replica hostnames, this module supervises a separate
  `Logflare.Repo` connection pool for each replica, registered under a local
  `Registry`. If no replicas are configured, the supervisor is skipped entirely.

  Replica pools are identified by their hostname. Callers can temporarily redirect
  Ecto queries to a replica for the duration of a function call using `apply/4`,
  which swaps the dynamic repo and restores it afterwards.
  """

  @registry __MODULE__.Registry

  def child_spec(options) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [options]},
      type: :supervisor
    }
  end

  def start_link(options) do
    hostnames = Keyword.fetch!(options, :hostnames)

    if hostnames == [] do
      :ignore
    else
      replicas =
        Enum.map(hostnames, fn hostname ->
          config = [hostname: hostname, name: {:via, Registry, {@registry, hostname}}]
          Supervisor.child_spec({Logflare.Repo, config}, id: hostname)
        end)

      children = [
        {Registry, name: @registry, keys: :unique}
        | replicas
      ]

      Supervisor.start_link(children, strategy: :one_for_one)
    end
  end

  @doc """
  Looks up the PID of the replica connection pool for the given hostname.
  Raises if no such replica is found.
  """
  def lookup!(hostname) do
    case Registry.lookup(@registry, hostname) do
      [{pid, _}] -> pid
      [] -> raise "unknown replica hostname: #{inspect(hostname)}"
    end
  end
end
