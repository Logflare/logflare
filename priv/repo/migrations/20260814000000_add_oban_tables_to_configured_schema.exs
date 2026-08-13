defmodule Logflare.Repo.Migrations.AddObanTablesToConfiguredSchema do
  use Ecto.Migration

  def up, do: Oban.Migration.up(version: 12, prefix: oban_prefix())
  def down, do: :ok

  defp oban_prefix, do: System.get_env("DB_SCHEMA", "public")
end
