defmodule Logflare.Repo.Migrations.AddObanTablesToConfiguredSchema do
  use Ecto.Migration

  alias Logflare.Repo.Migrator

  def up do
    Migrator.with_replicated_execute(fn ->
      Oban.Migration.up(version: 12, prefix: oban_prefix())
    end)
  end

  def down, do: :ok

  defp oban_prefix do
    :logflare
    |> Application.get_env(Logflare.Repo, [])
    |> Keyword.get(:schema)
    |> Kernel.||("public")
  end
end
