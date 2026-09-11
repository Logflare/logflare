defmodule Logflare.Release do
  require Logger
  @app :logflare

  def migrate do
    Logger.info("Starting migration")
    Application.ensure_all_started(:ssl)

    for repo <- repos() do
      migration_repo = Logflare.Repo.Migrator.migration_repo_for(repo)

      {:ok, _, _} =
        Ecto.Migrator.with_repo(migration_repo, &Ecto.Migrator.run(&1, :up, all: true))
    end

    Logger.info("Migration finished")
  end

  def rollback(repo, version) do
    {:ok, _, _} = Ecto.Migrator.with_repo(repo, &Ecto.Migrator.run(&1, :down, to: version))
  end

  defp repos do
    Application.fetch_env!(@app, :ecto_repos)
  end
end
