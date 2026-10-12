defmodule Logflare.Repo.Migrations.RedoThisOne do
  use Ecto.Migration

  alias Logflare.Repo.Migrator

  def down do
    Migrator.with_replicated_execute(fn ->
      execute("ALTER TABLE rules DROP CONSTRAINT rules_sink_fkey")

      alter table(:rules) do
        modify(:sink, references(:sources, column: :token, type: :uuid, on_delete: :nothing))
      end
    end)
  end

  def up do
    Migrator.with_replicated_execute(fn ->
      execute("ALTER TABLE rules DROP CONSTRAINT rules_sink_fkey")

      alter table(:rules) do
        modify(:sink, references(:sources, column: :token, type: :uuid, on_delete: :delete_all))
      end
    end)
  end
end
