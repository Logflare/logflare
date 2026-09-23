defmodule Logflare.Repo.Migrations.AlterRulesSetSinkSourceIdNotNull do
  use Ecto.Migration

  alias Logflare.Repo.Migrator

  def up do
    Migrator.with_replicated_execute(fn ->
      execute "alter table rules alter column sink set not null"
      execute "alter table rules alter column source_id set not null"
    end)
  end

  def down do
    Migrator.with_replicated_execute(fn ->
      execute "alter table rules alter column sink drop not null"
      execute "alter table rules alter column source_id drop not null"
    end)
  end
end
