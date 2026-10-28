defmodule Logflare.Repo.Migrations.IndexRulesBackendId do
  use Ecto.Migration

  def up do
    create index(:rules, [:backend_id],
             comment:
               "Avoids a full rules scan when finding source routing trees to invalidate after backend changes."
           )
  end

  def down do
    drop index(:rules, [:backend_id])
  end
end
