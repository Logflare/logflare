defmodule Logflare.Repo.Migrations.IndexRulesBackendId do
  use Ecto.Migration

  def up do
    create index(:rules, [:backend_id],
             comment: "Supports rule cache invalidation after backend changes."
           )
  end

  def down do
    drop index(:rules, [:backend_id])
  end
end
