defmodule Logflare.Repo.Migrations.AddKeyValuesToPublication do
  use Ecto.Migration

  @publications Application.compile_env(:logflare, Logflare.ContextCache.CacheBuster)[
                  :publications
                ]
  @table "key_values"

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
