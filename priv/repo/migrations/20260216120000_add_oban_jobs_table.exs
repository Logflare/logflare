defmodule Logflare.Repo.Migrations.AddObanJobsTable do
  use Ecto.Migration

  alias Logflare.Repo.Migrator

  # Oban creates types, functions and triggers through raw SQL, which is not replicated
  # by default.
  def up do
    Migrator.with_replicated_execute(fn ->
      Oban.Migration.up(version: 12, prefix: oban_prefix())
    end)
  end

  def down do
    Migrator.with_replicated_execute(fn ->
      Oban.Migration.down(version: 1, prefix: oban_prefix())
    end)
  end

  defp oban_prefix do
    :logflare
    |> Application.get_env(Logflare.Repo, [])
    |> Keyword.get(:schema)
    |> Kernel.||("public")
  end
end
