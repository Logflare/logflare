defmodule Logflare.Repo.Migrations.DeleteSourcesOnAccountDelete do
  use Ecto.Migration

  def down do
    execute("ALTER TABLE sources DROP CONSTRAINT sources_user_id_fkey")

    alter table(:sources) do
      modify(:user_id, references(:users, on_delete: :nothing))
    end
  end

  def up do
    execute("ALTER TABLE sources DROP CONSTRAINT sources_user_id_fkey")

    alter table(:sources) do
      modify(:user_id, references(:users, on_delete: :delete_all))
    end
  end
end
