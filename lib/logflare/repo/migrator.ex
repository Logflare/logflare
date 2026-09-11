defmodule Logflare.Repo.Migrator do
  @moduledoc """
  Chooses whether migrations run directly against `Logflare.Repo` or through
  `Logflare.Repo.Pglogical` (which replicates DDL via pglogical), based on
  the `LOGFLARE_PGLOGICAL_REPLICATE_DDL_COMMANDS_SETS` env var.
  """

  @default_search_path "public"

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
end
