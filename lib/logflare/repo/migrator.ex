defmodule Logflare.Repo.Migrator do
  @moduledoc """
  Chooses whether migrations run directly against `Logflare.Repo` or through
  `Logflare.Repo.Pglogical` (which replicates DDL via pglogical), based on
  the `LOGFLARE_PGLOGICAL_REPLICATE_DDL_COMMANDS_SETS` env var.
  """

  @default_search_path "public"
  @replicate_execute_key {__MODULE__, :replicate_execute?}

  @spec migration_repo_for(Ecto.Repo.t()) :: Ecto.Repo.t()
  def migration_repo_for(Logflare.Repo) do
    if replication_sets() == [] do
      Logflare.Repo
    else
      Logflare.Repo.Pglogical
    end
  end

  def migration_repo_for(other_repo), do: other_repo

  @spec replication_sets() :: [String.t()]
  def replication_sets do
    Application.get_env(:logflare, __MODULE__, [])[:replication_sets] || []
  end

  @doc """
  The search_path replicated DDL must run under, derived from `DB_SCHEMA`.

  pglogical applies replicated DDL with an empty search_path on both provider
  and subscriber, so it has to be restored explicitly.
  """
  @spec search_path() :: String.t()
  def search_path do
    case Application.get_env(:logflare, __MODULE__, [])[:search_path] do
      nil -> @default_search_path
      "" -> @default_search_path
      search_path -> search_path
    end
  end

  @doc """
  Runs `fun` with raw `Ecto.Migration.execute/1` SQL strings opted into pglogical
  replication, for migrations that must replicate DDL they do not author themselves
  (for example `Oban.Migration.up/1`, which creates types, functions and triggers
  through raw SQL).

  The flag is scoped to the calling process, which is the same process the migration
  runner flushes DDL from. Pending DDL is flushed before the block returns, because
  `Ecto.Migrator` would otherwise execute it after `up/0` or `down/0` has returned,
  with the flag already cleared.
  """
  @spec with_replicated_execute((-> result)) :: result when result: term()
  def with_replicated_execute(fun) when is_function(fun, 0) do
    previous = Process.put(@replicate_execute_key, true)

    try do
      result = fun.()
      flush_pending_ddl()
      result
    after
      if previous == nil do
        Process.delete(@replicate_execute_key)
      else
        Process.put(@replicate_execute_key, previous)
      end
    end
  end

  @spec replicate_execute?() :: boolean()
  def replicate_execute?, do: Process.get(@replicate_execute_key, false)

  defp flush_pending_ddl do
    if Process.get(:ecto_migration), do: Ecto.Migration.Runner.flush()

    :ok
  end
end
