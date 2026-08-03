defmodule Logflare.Repo.Migrations.RedoThisOne do
  use Ecto.Migration

  def down do
    execute("ALTER TABLE rules DROP CONSTRAINT rules_sink_fkey")

    alter table(:rules) do
      modify(:sink, references(:sources, column: :token, type: :uuid, on_delete: :nothing))
    end
  end

  def up do
    execute("ALTER TABLE rules DROP CONSTRAINT rules_sink_fkey")

    alter table(:rules) do
      modify(:sink, references(:sources, column: :token, type: :uuid, on_delete: :delete_all))
    end
  end
end
