defmodule Logflare.Repo.Migrations.AddObanJobsTable do
  use Ecto.Migration

  alias Logflare.Repo.Migrator

  # Oban creates types, functions and triggers through raw SQL, which is not replicated
  # by default.
  def up do
    Migrator.with_replicated_execute(fn ->
      Oban.Migration.up(version: 12)
    end)
  end

  def down do
    Migrator.with_replicated_execute(fn ->
      Oban.Migration.down(version: 1)
    end)
  end
end
