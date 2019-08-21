defmodule Logflare.Tasks.ReleaseTasks do
  @start_apps [
    :postgrex,
    :ecto
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
    IO.puts("Booting pre hook...")
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
  end

  defp run_migrations() do
    IO.puts("Running migrations...")
    Ecto.Migrator.run(@repo, migrations_path(), :up, all: true)
  end

  defp migrations_path(), do: Path.join([priv_dir(), "repo", "migrations"])

  defp priv_dir(), do: "#{:code.priv_dir(@otp_app)}"
end
