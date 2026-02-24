defmodule Logflare.Repo.Migrations.SetRulesToReplicaIdentityFull do
  use Ecto.Migration

  def up do
    execute("alter table rules replica identity full")
  end

  def down do
    execute("alter table rules replica identity default")
  end
end
