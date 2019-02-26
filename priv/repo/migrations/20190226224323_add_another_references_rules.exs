defmodule Logflare.Repo.Migrations.AddAnotherReferencesRules do
  use Ecto.Migration

  def change do
    create(unique_index(:sources, [:token]))

    alter table(:rules) do
      modify(:sink, references(:sources, column: :token, type: :uuid, on_delete: :nothing))
    end
  end
end
