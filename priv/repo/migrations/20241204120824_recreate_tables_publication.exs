defmodule Logflare.Repo.Migrations.RecreateTablesPublication do
  use Ecto.Migration
  use Ecto.Migration

  @publications Application.get_env(:logflare, Logflare.ContextCache.CacheBuster)[:publications]
  @table "oauth_access_tokens"

  def up do
    for p <- @publications do
      execute("ALTER PUBLICATION #{p} ADD TABLE #{@table};")
    end
  end

  def down do
    for p <- @publications do
      execute("ALTER PUBLICATION #{p} DROP TABLE #{@table};")
    end
  end
end
