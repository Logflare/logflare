defmodule Logflare.Repo.Migrations.AddObanTablesToConfiguredSchema do
  use Ecto.Migration

  def up, do: Oban.Migration.up(version: 12, prefix: oban_prefix())
  def down, do: :ok

  defp oban_prefix do
    :logflare
    |> Application.get_env(Logflare.Repo, [])
    |> Keyword.get(:schema)
    |> Kernel.||("public")
  end
end
