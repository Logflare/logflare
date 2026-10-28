defmodule Logflare.Repo.Migrations.IndexRulesBackendId do
  use Ecto.Migration

  def up do
    # Avoids a full rules scan when invalidating source trees after a backend change.
    create index(:rules, [:backend_id])
  end

  def down do
    drop index(:rules, [:backend_id])
  end
end
