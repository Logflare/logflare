defmodule Logflare.Repo.Migrations.AlterRulesSetSinkSourceIdNotNull do
  use Ecto.Migration

  def up do
    execute "alter table rules alter column sink set not null"
    execute "alter table rules alter column source_id set not null"
  end

  def down do
    execute "alter table rules alter column sink drop not null"
    execute "alter table rules alter column source_id drop not null"
  end
end
