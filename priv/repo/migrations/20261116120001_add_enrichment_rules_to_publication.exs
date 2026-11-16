defmodule Logflare.Repo.Migrations.AddEnrichmentRulesToPublication do
  use Ecto.Migration

  @publications Application.compile_env(:logflare, Logflare.ContextCache.CacheBuster)[
                  :publications
                ]
  @table "enrichment_rules"

  def up do
    for publication <- @publications do
      execute("ALTER PUBLICATION #{publication} ADD TABLE #{@table};")
    end
  end

  def down do
    for publication <- @publications do
      execute("ALTER PUBLICATION #{publication} DROP TABLE #{@table};")
    end
  end
end
