defmodule Logflare.Repo.Migrations.SourceReplicaIdentityFull do
  use Ecto.Migration
    def up do
      execute("alter table sources replica identity full")
    end

    def down do
      execute("alter table sources replica identity default")
    end
end
