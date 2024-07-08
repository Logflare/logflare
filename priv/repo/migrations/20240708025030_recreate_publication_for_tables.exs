defmodule Logflare.Repo.Migrations.RecreatePublicationForTables do
  use Ecto.Migration
  use Ecto.Migration

  @publications Application.get_env(:logflare, Logflare.CacheBuster)[:publications]
  @publication_tables Application.get_env(:logflare, Logflare.CacheBuster)[:publication_tables]

  def up do
    for p <- @publications, do: execute("DROP PUBLICATION #{p};")
    for p <- @publications do
      tables = Enum.join(@publication_tables, ", ")
      execute("CREATE PUBLICATION #{p} FOR TABLE #{tables};")
    end
  end
  def down do
    :noop
  end


end
