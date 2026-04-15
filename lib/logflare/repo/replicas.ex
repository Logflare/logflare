defmodule Logflare.Repo.Replicas do
  @moduledoc """
  Manages a pool of PostgreSQL read replica connections for `Logflare.Repo`.

  When started with one or more replica URLs, this module supervises a separate
  `Logflare.Repo` connection pool for each replica, registered under a local
  `Registry`. If no replicas are configured, the supervisor is skipped entirely.

  Replica pools are identified by their URL. Callers can temporarily redirect
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
    urls = Keyword.fetch!(options, :urls)

    if urls == [] do
      :ignore
    else
      replicas =
        Enum.map(urls, fn url ->
          spec = {Logflare.Repo, url: url, name: {:via, Registry, {@registry, url}}}
          Supervisor.child_spec(spec, id: url)
        end)

      children = [
        {Registry, name: @registry, keys: :unique}
        | replicas
      ]

      Supervisor.start_link(children, strategy: :one_for_one)
    end
  end

  @doc """
  Applies the given MFA using the connection pool for `replica`.

  Temporarily sets the process-local dynamic repo to the pool registered under
  `replica`, restoring the previous value afterwards.
  """
  def apply(url, m, f, a) do
    prev_repo = Logflare.Repo.get_dynamic_repo()

    pid =
      case Registry.lookup(@registry, url) do
        [{pid, _}] -> pid
        [] -> raise "unknown replica #{inspect(url)}"
      end

    Logflare.Repo.put_dynamic_repo(pid)

    try do
      apply(m, f, a)
    after
      Logflare.Repo.put_dynamic_repo(prev_repo)
    end
  end
end
