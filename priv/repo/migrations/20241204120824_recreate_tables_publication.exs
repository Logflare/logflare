defmodule Logflare.Repo.Migrations.RecreateTablesPublication do
  use Ecto.Migration
  use Ecto.Migration

  @publications Application.get_env(:logflare, Logflare.ContextCache.CacheBuster)[:publications]
  @prev_publication_tables [
    "billing_accounts",
    "plans",
    "rules",
    "source_schemas",
    "sources",
    "users",
    "backends",
    "team_users"
  ]
  @new_publication_tables @prev_publication_tables ++ ["oauth_access_tokens"]

  def up do
    for p <- @publications do
      tables = Enum.join(@new_publication_tables, ", ")
      execute("ALTER PUBLICATION #{p} FOR TABLE #{tables};")
    end
  end

  def down do
    for p <- @publications do
      tables = Enum.join(@prev_publication_tables, ", ")
      execute("ALTER PUBLICATION #{p} FOR TABLE #{tables};")
    end
  end
end
