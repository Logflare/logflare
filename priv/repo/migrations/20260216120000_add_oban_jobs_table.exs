defmodule Logflare.Repo.Migrations.AddObanJobsTable do
  use Ecto.Migration

  def up, do: Oban.Migration.up(version: 12, prefix: oban_prefix())
  def down, do: Oban.Migration.down(version: 1, prefix: oban_prefix())

  defp oban_prefix do
    :logflare
    |> Application.get_env(Logflare.Repo, [])
    |> Keyword.get(:schema)
    |> Kernel.||("public")
  end
end
