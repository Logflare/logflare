defmodule Logflare.Repo.Migrations.DeleteSourcesOnAccountDelete do
  use Ecto.Migration

  alias Logflare.Repo.Migrator

  def down do
    Migrator.with_replicated_execute(fn ->
      execute("ALTER TABLE sources DROP CONSTRAINT sources_user_id_fkey")

      alter table(:sources) do
        modify(:user_id, references(:users, on_delete: :nothing))
      end
    end)
  end

  def up do
    Migrator.with_replicated_execute(fn ->
      execute("ALTER TABLE sources DROP CONSTRAINT sources_user_id_fkey")

      alter table(:sources) do
        modify(:user_id, references(:users, on_delete: :delete_all))
      end
    end)
  end
end
