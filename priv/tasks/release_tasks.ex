defmodule Logflare.Tasks.ReleaseTasks do
  @moduledoc false
  @start_apps [
    :postgrex,
    :ecto,
    :ssl
  ]

  @repo Logflare.Repo

  @otp_app :logflare

  def setup do
    boot()
    create_database()
    start_connection()
    run_migrations()
  end

  defp boot() do
    IO.puts("Starting pre-boot release task...")
    # Load app without starting it
    :ok = Application.load(@otp_app)
    # Ensure postgrex and ecto applications started
    Enum.each(@start_apps, &Application.ensure_all_started/1)
  end

  defp create_database() do
    IO.puts("Creating the database if needed...")
    @repo.__adapter__.storage_up(@repo.config)
  end

  defp start_connection() do
    {:ok, _} = @repo.start_link(pool_size: 1)

    migration_repo = Logflare.Repo.Migrator.migration_repo_for(@repo)

    if migration_repo != @repo do
      {:ok, _} = migration_repo.start_link(pool_size: 2)
    end
  end

  defp run_migrations() do
    IO.puts("Running migrations...")
    migration_repo = Logflare.Repo.Migrator.migration_repo_for(@repo)
    Ecto.Migrator.run(migration_repo, migrations_path(), :up, all: true)
  end

  defp migrations_path(), do: Path.join([priv_dir(), "repo", "migrations"])

  defp priv_dir(), do: "#{:code.priv_dir(@otp_app)}"
end
